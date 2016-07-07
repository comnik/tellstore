/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */

#include <tellstore/ClientSocket.hpp>
#include <tellstore/AbstractTuple.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>
#include <commitmanager/ClientSocket.hpp>

#include <crossbow/alignment.hpp>
#include <crossbow/infinio/Endpoint.hpp>
#include <crossbow/infinio/InfinibandBuffer.hpp>
#include <crossbow/logger.hpp>

namespace tell {
namespace store {
namespace {

void writeSnapshot(crossbow::buffer_writer& message, const commitmanager::SnapshotDescriptor& snapshot) {
    // TODO Implement snapshot caching
    message.write<uint8_t>(0x0u); // Cached
    message.write<uint8_t>(0x1u); // HasDescriptor
    message.align(sizeof(uint64_t));
    snapshot.serialize(message);
}

} // anonymous namespace

template<class ResponseType>
std::shared_ptr<ResponseType> ClusterResponse<ResponseType>::get () {
    if (this->mFuture.which() == 1) {
        // First we have to wait for the cluster state request to finish
        if (!this->mStatusResponse->waitForResult()) {
            auto& ec = this->mStatusResponse->error();
            LOG_ERROR("Error while receiving cluster information [error = %1% %2%]", ec, ec.message());
        }
        auto clusterMeta = this->mStatusResponse->get();

        mConfigUpdateHandler(std::move(clusterMeta));
    }

    return boost::apply_visitor(ClusterResponse::future_visitor(), this->mFuture);
}

void CreateTableResponse::processResponse(crossbow::buffer_reader& message) {
    auto tableId = message.read<uint64_t>();
    setResult(tableId);
}

void GetTablesResponse::processResponse(crossbow::buffer_reader& message) {
    std::vector<Table> result;

    auto resultSize = message.read<uint64_t>();
    result.reserve(resultSize);

    for (decltype(resultSize) i = 0; i < resultSize; ++i) {
        auto tableId = message.read<uint64_t>();

        auto tableNameLength = message.read<uint32_t>();
        crossbow::string tableName(message.read(tableNameLength), tableNameLength);
        message.align(8u);

        auto schema = Schema::deserialize(message);

        result.emplace_back(tableId, std::move(tableName), std::move(schema));
    }

    setResult(std::move(result));
}

void GetTableResponse::processResponse(crossbow::buffer_reader& message) {
    auto tableId = message.read<uint64_t>();
    auto schema = Schema::deserialize(message);

    setResult(tableId, std::move(mTableName), std::move(schema));
}

void GetResponse::processResponse(crossbow::buffer_reader& message) {
    setResult(Tuple::deserialize(message));
}

void ModificationResponse::processResponse(crossbow::buffer_reader& /* message */) {
    // Nothing to do
}

ScanResponse::ScanResponse(crossbow::infinio::Fiber& fiber, std::shared_ptr<ScanIterator> iterator,
        ClientSocket& socket, ScanMemory memory, uint16_t scanId)
        : crossbow::infinio::RpcResponse(fiber),
          mIterator(std::move(iterator)),
          mSocket(socket),
          mMemory(std::move(memory)),
          mScanId(scanId),
          mOffsetRead(0u),
          mOffsetWritten(0u) {
    LOG_ASSERT(mMemory.valid(), "Memory not valid");
}

std::tuple<const char*, const char*> ScanResponse::nextChunk() {
    LOG_ASSERT(available(), "No data available");

    auto start = reinterpret_cast<const char*>(mMemory.data()) + mOffsetRead;
    auto end = reinterpret_cast<const char*>(mMemory.data()) + mOffsetWritten;
    mOffsetRead = mOffsetWritten;

    if (!done()) {
        mSocket.scanProgress(mScanId, shared_from_this(), mOffsetRead);
    }

    return std::make_tuple(start, end);
}

void ScanResponse::onResponse(uint32_t messageType, crossbow::buffer_reader& message) {
    if (messageType == std::numeric_limits<uint32_t>::max()) {
        onAbort(std::error_code(message.read<uint64_t>(), error::get_error_category()));
        return;
    }
    if (messageType != crossbow::to_underlying(ResponseType::SCAN)) {
        onAbort(crossbow::infinio::error::wrong_type);
        return;
    }

    // The scan already completed successfully
    if (done()) {
        return;
    }
    auto scanDone = (message.read<uint8_t>() != 0u);

    message.advance(sizeof(size_t) - sizeof(uint8_t));
    auto offset = message.read<size_t>();
    if (mOffsetWritten < offset) {
        mOffsetWritten = offset;
    }

    if (scanDone) {
        complete();
        mSocket.scanComplete(mScanId);
    }

    if (auto it = mIterator.lock()) {
        it->notify();
    }
}

void ScanResponse::onAbort(std::error_code ec) {
    // The scan already completed successfully
    if (done()) {
        return;
    }
    complete();
    mSocket.scanComplete(mScanId);

    if (auto it = mIterator.lock()) {
        it->abort(std::move(ec));
    }
}

ScanIterator::ScanIterator(crossbow::infinio::Fiber& fiber, Record record, size_t shardSize)
        : mFiber(fiber),
          mRecord(std::move(record)),
          mWaiting(false),
          mChunkPos(nullptr),
          mChunkEnd(nullptr) {
    mScans.reserve(shardSize);
}

bool ScanIterator::hasNext() {
    if (mError) {
        throw std::system_error(mError);
    }
    while (mChunkPos == nullptr) {
        auto done = true;
        for (auto& response : mScans) {
            done = (done && response->done());
            if (!response->available()) {
                continue;
            }
            std::tie(mChunkPos, mChunkEnd) = response->nextChunk();
            return true;
        }
        if (done) {
            return false;
        }
        mWaiting = true;
        mFiber.wait();
        mWaiting = false;
    }
    return true;
}

std::tuple<uint64_t, const char*, size_t> ScanIterator::next() {
    if (!hasNext()) {
        throw std::out_of_range("Can not iterate past the last element");
    }

    auto key = *reinterpret_cast<const uint64_t*>(mChunkPos);
    mChunkPos += sizeof(uint64_t);

    auto data = mChunkPos;
    auto length = mRecord.sizeOfTuple(data);
    LOG_ASSERT(length % 8 == 0, "Data must be 8 byte padded");
    mChunkPos += length;

    if (mChunkPos >= mChunkEnd) {
        LOG_ASSERT(mChunkPos == mChunkEnd, "Chunk pointer not pointing to the exact end of the chunk");
        mChunkPos = nullptr;
    }

    return std::make_tuple(key, data, length);
}

std::tuple<const char*, const char*> ScanIterator::nextChunk() {
    if (!hasNext()) {
        throw std::out_of_range("Can not iterate past the last element");
    }

    auto chunk = std::make_tuple(mChunkPos, mChunkEnd);
    mChunkPos = nullptr;
    return chunk;
}

void ScanIterator::wait() {
    for (auto& response : mScans) {
        while (!response->wait());
    }
}

void ScanIterator::notify() {
    if (mWaiting) {
        mFiber.resume();
    }
}

void ScanIterator::abort(std::error_code ec) {
    LOG_ERROR("Scan aborted with error [error = %1% %2%]", ec, ec.message());
    if (!mError) {
        mError = std::move(ec);
    }
    notify();
}

void ClientSocket::connect(const crossbow::infinio::Endpoint& host, uint64_t threadNum) {
    LOG_DEBUG("Connecting to TellStore server %1% on processor %2%", host, threadNum);

    auto data = handshakeString();
    data.append(reinterpret_cast<char*>(&threadNum), sizeof(uint64_t));

    crossbow::infinio::RpcClientSocket::connect(host, data);
}

void ClientSocket::shutdown() {
    LOG_DEBUG("Shutting down TellStore connection");
    crossbow::infinio::RpcClientSocket::shutdown();
}

std::shared_ptr<CreateTableResponse> ClientSocket::createTable(crossbow::infinio::Fiber& fiber,
        const crossbow::string& name, const Schema& schema) {
    auto response = std::make_shared<CreateTableResponse>(fiber);

    auto nameLength = name.size();
    auto schemaLength = schema.serializedLength();
    uint32_t messageLength = sizeof(uint32_t) + nameLength;
    messageLength = crossbow::align(messageLength, sizeof(uint64_t));
    messageLength += schemaLength;

    sendRequest(response, RequestType::CREATE_TABLE, messageLength, [nameLength, schemaLength, &name, &schema]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint32_t>(nameLength);
        message.write(name.data(), nameLength);

        message.align(sizeof(uint64_t));
        schema.serialize(message);
    });

    return response;
}

std::shared_ptr<GetTablesResponse> ClientSocket::getTables(crossbow::infinio::Fiber& fiber) {
    auto response = std::make_shared<GetTablesResponse>(fiber);

    sendRequest(response, RequestType::GET_TABLES, 0u, []
            (crossbow::buffer_writer& /* message */, std::error_code& /* ec */) {
    });

    return response;
}

std::shared_ptr<GetTableResponse> ClientSocket::getTable(crossbow::infinio::Fiber& fiber,
        const crossbow::string& name) {
    auto response = std::make_shared<GetTableResponse>(fiber, name);

    auto nameLength = name.size();
    uint32_t messageLength = sizeof(uint32_t) + nameLength;

    sendRequest(response, RequestType::GET_TABLE, messageLength, [nameLength, &name]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint32_t>(nameLength);
        message.write(name.data(), nameLength);
    });

    return response;
}

std::shared_ptr<GetResponse> ClientSocket::get(crossbow::infinio::Fiber& fiber, uint64_t tableId, uint64_t key,
        const commitmanager::SnapshotDescriptor& snapshot) {
    auto response = std::make_shared<GetResponse>(fiber);

    uint32_t messageLength = 3 * sizeof(uint64_t) + snapshot.serializedLength();

    sendRequest(response, RequestType::GET, messageLength, [tableId, key, &snapshot]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tableId);
        message.write<uint64_t>(key);
        writeSnapshot(message, snapshot);
    });

    return response;
}

std::shared_ptr<ModificationResponse> ClientSocket::insert(crossbow::infinio::Fiber& fiber, uint64_t tableId,
        uint64_t key, const commitmanager::SnapshotDescriptor& snapshot, const AbstractTuple& tuple) {
    auto response = std::make_shared<ModificationResponse>(fiber);

    auto tupleLength = tuple.size();
    LOG_ASSERT(tupleLength % 8 == 0, "Data must be 8 byte padded");

    uint32_t messageLength = 4 * sizeof(uint64_t) + tupleLength + snapshot.serializedLength();
    sendRequest(response, RequestType::INSERT, messageLength, [tableId, key, tupleLength, &tuple, &snapshot]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tableId);
        message.write<uint64_t>(key);

        message.write<uint32_t>(0x0u);
        message.write<uint32_t>(tupleLength);
        tuple.serialize(message.data());
        message.advance(tupleLength);

        writeSnapshot(message, snapshot);
    });

    return response;
}

std::shared_ptr<ModificationResponse> ClientSocket::update(crossbow::infinio::Fiber& fiber, uint64_t tableId,
        uint64_t key, const commitmanager::SnapshotDescriptor& snapshot, const AbstractTuple& tuple) {
    auto response = std::make_shared<ModificationResponse>(fiber);

    auto tupleLength = tuple.size();
    LOG_ASSERT(tupleLength % 8 == 0, "Data must be 8 byte padded");

    uint32_t messageLength = 4 * sizeof(uint64_t) + tupleLength + snapshot.serializedLength();
    sendRequest(response, RequestType::UPDATE, messageLength, [tableId, key, tupleLength, &tuple, &snapshot]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tableId);
        message.write<uint64_t>(key);

        message.write<uint32_t>(0x0u);
        message.write<uint32_t>(tupleLength);
        tuple.serialize(message.data());
        message.advance(tupleLength);

        writeSnapshot(message, snapshot);
    });

    return response;
}

std::shared_ptr<ModificationResponse> ClientSocket::remove(crossbow::infinio::Fiber& fiber, uint64_t tableId,
        uint64_t key, const commitmanager::SnapshotDescriptor& snapshot) {
    auto response = std::make_shared<ModificationResponse>(fiber);

    uint32_t messageLength = 3 * sizeof(uint64_t) + snapshot.serializedLength();

    sendRequest(response, RequestType::REMOVE, messageLength, [tableId, key, &snapshot]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tableId);
        message.write<uint64_t>(key);
        writeSnapshot(message, snapshot);
    });

    return response;
}

std::shared_ptr<ModificationResponse> ClientSocket::revert(crossbow::infinio::Fiber& fiber, uint64_t table,
        uint64_t key, const commitmanager::SnapshotDescriptor& snapshot) {
    auto response = std::make_shared<ModificationResponse>(fiber);

    uint32_t messageLength = 3 * sizeof(uint64_t) + snapshot.serializedLength();

    sendRequest(response, RequestType::REVERT, messageLength, [table, key, &snapshot]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(table);
        message.write<uint64_t>(key);
        writeSnapshot(message, snapshot);
    });

    return response;
}

void ClientSocket::scanStart(uint16_t scanId,
                             std::shared_ptr<ScanResponse> response,
                             uint64_t tableId,
                             ScanQueryType queryType,
                             uint32_t selectionLength,
                             const char* selection,
                             uint32_t queryLength,
                             const char* query,
                             const commitmanager::SnapshotDescriptor& snapshot) {
    if (!startAsyncRequest(scanId, response)) {
        response->onAbort(error::invalid_scan);
        return;
    }

    uint32_t messageLength = 6 * sizeof(uint64_t) + selectionLength + queryLength;
    messageLength = crossbow::align(messageLength, sizeof(uint64_t));
    messageLength += sizeof(uint64_t) + snapshot.serializedLength();

    sendAsyncRequest(scanId, response, RequestType::SCAN, messageLength,
            [response, tableId, queryType, selectionLength, selection, queryLength, query, &snapshot]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tableId);
        message.write<uint8_t>(crossbow::to_underlying(queryType));

        auto& memory = response->scanMemory();
        message.set(0, sizeof(uint64_t) - sizeof(uint8_t));
        message.write<uint64_t>(reinterpret_cast<uintptr_t>(memory.data()));
        message.write<uint64_t>(memory.length());
        message.write<uint32_t>(memory.key());

        message.write<uint32_t>(selectionLength);
        message.write(selection, selectionLength);

        message.set(0, sizeof(uint32_t));
        message.write<uint32_t>(queryLength);
        message.write(query, queryLength);

        message.align(sizeof(uint64_t));
        writeSnapshot(message, snapshot);
    });
}

void ClientSocket::scanProgress(uint16_t scanId, std::shared_ptr<ScanResponse> response, size_t offset) {
    uint32_t messageLength = sizeof(size_t);

    sendAsyncRequest(scanId, response, RequestType::SCAN_PROGRESS, messageLength, [offset]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<size_t>(offset);
    });
}

void ClientSocket::transferKeys(commitmanager::Hash rangeStart,
                                commitmanager::Hash rangeEnd, 
                                uint16_t scanId,
                                std::shared_ptr<ScanResponse> response,
                                uint64_t tableId,
                                ScanQueryType queryType, 
                                uint32_t selectionLength,
                                const char* selection,
                                uint32_t queryLength,
                                const char* query,
                                const commitmanager::SnapshotDescriptor& snapshot) {
    
    if (!startAsyncRequest(scanId, response)) {
        response->onAbort(error::invalid_scan);
        return;
    }

    uint32_t messageLength = 2*sizeof(commitmanager::Hash) + 6*sizeof(uint64_t) + selectionLength + queryLength;
    messageLength = crossbow::align(messageLength, sizeof(uint64_t));
    messageLength += sizeof(uint64_t) + snapshot.serializedLength();

    sendAsyncRequest(scanId, response, RequestType::KEY_TRANSFER, messageLength,
            [rangeStart, rangeEnd, response, tableId, queryType, selectionLength, selection, queryLength, query, &snapshot]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<commitmanager::Hash>(rangeStart);
        message.write<commitmanager::Hash>(rangeEnd);

        message.write<uint64_t>(tableId);
        message.write<uint8_t>(crossbow::to_underlying(queryType));

        auto& memory = response->scanMemory();
        message.set(0, sizeof(uint64_t) - sizeof(uint8_t));
        message.write<uint64_t>(reinterpret_cast<uintptr_t>(memory.data()));
        message.write<uint64_t>(memory.length());
        message.write<uint32_t>(memory.key());

        message.write<uint32_t>(selectionLength);
        message.write(selection, selectionLength);

        message.set(0, sizeof(uint32_t));
        message.write<uint32_t>(queryLength);
        message.write(query, queryLength);

        message.align(sizeof(uint64_t));
        writeSnapshot(message, snapshot);
    });
}

template class ClusterResponse<GetResponse>;
template class ClusterResponse<ModificationResponse>;

} // namespace store
} // namespace tell
