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
 *     Nikolas Göbel <ngoebel@inf.ethz.ch>
 */

#include "ServerSocket.hpp"

#include <util/PageManager.hpp>

#include <tellstore/ErrorCode.hpp>
#include <tellstore/MessageTypes.hpp>
#include <tellstore/TransactionRunner.hpp>

#include <crossbow/enum_underlying.hpp>
#include <crossbow/infinio/InfinibandBuffer.hpp>
#include <crossbow/logger.hpp>

#include <set>
#include <vector>
#include <signal.h>

namespace tell {
namespace store {

using namespace tell::commitmanager;
using namespace std::placeholders;


static int SIGNAL_INTERRUPTED = 0;
static void signalHandler(int signalValue) {
    if (SIGNAL_INTERRUPTED == 0) {
        LOG_INFO("Caught signal");
        SIGNAL_INTERRUPTED = 1;
    } else {
        std::exit(0);
    }
}

static void catchSignals(void) {
    struct sigaction action;
    action.sa_handler = signalHandler;
    action.sa_flags = 0;
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);
}


void ServerSocket::writeScanProgress(uint16_t scanId, bool done, size_t offset) {
    uint32_t messageLength = 2 * sizeof(size_t);
    writeResponse(crossbow::infinio::MessageId(scanId, true), ResponseType::SCAN, messageLength, [done, offset]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint8_t>(done ? 0x1u : 0x0u);
        message.set(0, sizeof(size_t) - sizeof(uint8_t));
        message.write<size_t>(offset);
    });
}

void ServerSocket::onRequest(crossbow::infinio::MessageId messageId, uint32_t messageType,
        crossbow::buffer_reader& request) {
#ifdef NDEBUG
#else
    LOG_TRACE("MID %1%] Handling request of type %2%", messageId.userId(), messageType);
    auto startTime = std::chrono::steady_clock::now();
#endif

    switch (messageType) {

    case crossbow::to_underlying(RequestType::CREATE_TABLE): {
        handleCreateTable(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::GET_TABLES): {
        handleGetTables(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::GET_TABLE): {
        handleGetTable(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::GET): {
        handleGet(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::UPDATE): {
        handleUpdate(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::INSERT): {
        handleInsert(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::REMOVE): {
        handleRemove(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::REVERT): {
        handleRevert(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::SCAN): {
        handleScan(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::SCAN_PROGRESS): {
        handleScanProgress(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::KEY_TRANSFER): {
        handleKeyTransfer(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::REQUEST_TRANSFER): {
        handleRequestTransfer(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::COMMIT): {
        // TODO Implement commit logic
    } break;

    default: {
        writeErrorResponse(messageId, error::unkown_request);
    } break;
    }

    if (SIGNAL_INTERRUPTED) {
        manager().shutdown();
    }

#ifdef NDEBUG
#else
    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
    LOG_TRACE("MID %1%] Handling request took %2%ns", messageId.userId(), duration.count());
#endif
}

void ServerSocket::handleCreateTable(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableNameLength = request.read<uint32_t>();
    crossbow::string tableName(request.read(tableNameLength), tableNameLength);

    request.align(sizeof(uint64_t));
    auto schema = Schema::deserialize(request);

    uint64_t tableId = 0;
    auto succeeded = mStorage.createTable(tableName, schema, tableId);
    LOG_ASSERT((tableId != 0) || !succeeded, "Table ID of 0 does not denote failure");

    if (!succeeded) {
        writeErrorResponse(messageId, error::invalid_table);
        return;
    }

    uint32_t messageLength = sizeof(uint64_t);
    writeResponse(messageId, ResponseType::CREATE_TABLE, messageLength, [tableId]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tableId);
    });
}

void ServerSocket::handleGetTables(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tables = mStorage.getTables();

    uint32_t messageLength = sizeof(uint64_t);
    for (auto table : tables) {
        messageLength += sizeof(uint64_t) + sizeof(uint32_t);
        messageLength += table->tableName().size();
        messageLength = crossbow::align(messageLength, 8u);
        messageLength += table->schema().serializedLength();
    }

    writeResponse(messageId, ResponseType::GET_TABLES, messageLength, [&tables]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tables.size());
        for (auto table : tables) {
            message.write<uint64_t>(table->tableId());

            auto& tableName = table->tableName();
            message.write<uint32_t>(tableName.size());
            message.write(tableName.data(), tableName.size());
            message.align(8u);

            auto& schema = table->schema();
            schema.serialize(message);
        }
    });
}

void ServerSocket::handleGetTable(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableNameLength = request.read<uint32_t>();
    crossbow::string tableName(request.read(tableNameLength), tableNameLength);

    uint64_t tableId = 0x0u;
    auto table = mStorage.getTable(tableName, tableId);

    if (!table) {
        writeErrorResponse(messageId, error::invalid_table);
        return;
    }

    auto& schema = table->schema();

    uint32_t messageLength = sizeof(uint64_t) + schema.serializedLength();
    writeResponse(messageId, ResponseType::GET_TABLE, messageLength, [tableId, &schema]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tableId);
        schema.serialize(message);
    });
}

void ServerSocket::handleGet(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableId = request.read<uint64_t>();
    auto key = request.read<uint64_t>();
    handleSnapshot(messageId, request, [this, messageId, tableId, key]
            (const SnapshotDescriptor& snapshot) {
        auto ec = mStorage.get(tableId, key, snapshot, [this, messageId]
                (size_t size, uint64_t version, bool isNewest) {
            char* data = nullptr;
            // Message size is 8 bytes version plus 8 bytes (isNewest, size) and data
            uint32_t messageLength = 2 * sizeof(uint64_t) + size;
            writeResponse(messageId, ResponseType::GET, messageLength, [size, version, isNewest, &data]
                    (crossbow::buffer_writer& message, std::error_code& /* ec */) {
                message.write<uint64_t>(version);
                message.write<uint8_t>(isNewest ? 0x1u : 0x0u);
                message.set(0, sizeof(uint32_t) - sizeof(uint8_t));
                message.write<uint32_t>(size);
                data = message.data();
            });
            return data;
        });

        if (ec) {
            writeErrorResponse(messageId, static_cast<error::errors>(ec));
            return;
        }
    });
}

void ServerSocket::handleUpdate(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableId = request.read<uint64_t>();
    auto key = request.read<uint64_t>();

    request.advance(sizeof(uint32_t));
    auto dataLength = request.read<uint32_t>();
    auto data = request.read(dataLength);
    request.align(8u);

    handleSnapshot(messageId, request, [this, messageId, tableId, key, dataLength, data]
            (const SnapshotDescriptor& snapshot) {
        auto ec = mStorage.update(tableId, key, dataLength, data, snapshot);
        writeModificationResponse(messageId, ec);
    });
}

void ServerSocket::handleInsert(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableId = request.read<uint64_t>();
    auto key = request.read<uint64_t>();

    request.advance(sizeof(uint32_t));
    auto dataLength = request.read<uint32_t>();
    auto data = request.read(dataLength);
    request.align(8u);

    handleSnapshot(messageId, request, [this, messageId, tableId, key, dataLength, data]
            (const SnapshotDescriptor& snapshot) {
        auto ec = mStorage.insert(tableId, key, dataLength, data, snapshot);
        writeModificationResponse(messageId, ec);
    });
}

void ServerSocket::handleRemove(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableId = request.read<uint64_t>();
    auto key = request.read<uint64_t>();

    handleSnapshot(messageId, request, [this, messageId, tableId, key]
            (const SnapshotDescriptor& snapshot) {
        auto ec = mStorage.remove(tableId, key, snapshot);
        writeModificationResponse(messageId, ec);
    });
}

void ServerSocket::handleRevert(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableId = request.read<uint64_t>();
    auto key = request.read<uint64_t>();

    handleSnapshot(messageId, request, [this, messageId, tableId, key]
            (const SnapshotDescriptor& snapshot) {
        auto ec = mStorage.revert(tableId, key, snapshot);
        writeModificationResponse(messageId, ec);
    });
}

void ServerSocket::handleScan(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableId = request.read<uint64_t>();
    auto queryType = crossbow::from_underlying<ScanQueryType>(request.read<uint8_t>());

    request.advance(sizeof(uint64_t) - sizeof(uint8_t));
    auto remoteAddress = request.read<uint64_t>();
    auto remoteLength = request.read<uint64_t>();
    auto remoteKey = request.read<uint32_t>();
    crossbow::infinio::RemoteMemoryRegion remoteRegion(remoteAddress, remoteLength, remoteKey);

    auto selectionLength = request.read<uint32_t>();
    if (selectionLength % 8u != 0u || selectionLength < 16u) {
        writeErrorResponse(messageId, error::invalid_scan);
        return;
    }
    auto selectionData = request.read(selectionLength);
    std::unique_ptr<char[]> selection(new char[selectionLength]);
    memcpy(selection.get(), selectionData, selectionLength);

    request.advance(sizeof(uint32_t));
    auto queryLength = request.read<uint32_t>();
    auto queryData = request.read(queryLength);
    std::unique_ptr<char[]> query(new char[queryLength]);
    memcpy(query.get(), queryData, queryLength);

    request.align(sizeof(uint64_t));
    handleSnapshot(messageId, request,
            [this, messageId, tableId, &remoteRegion, selectionLength, &selection, queryType, queryLength, &query]
            (const SnapshotDescriptor& snapshot) {

        auto scanId = static_cast<uint16_t>(messageId.userId() & 0xFFFFu);

        // Copy snapshot descriptor
        auto scanSnapshot = SnapshotDescriptor::create(snapshot.lowestActiveVersion(),
                snapshot.baseVersion(), snapshot.version(), snapshot.data());
        
        auto table = mStorage.getTable(tableId);

        std::unique_ptr<ServerScanQuery> scanData(
            new ServerScanQuery(scanId, 
                                queryType, 
                                std::move(selection),
                                selectionLength, 
                                std::move(query), 
                                queryLength, 
                                std::move(scanSnapshot), 
                                table->record(),
                                manager().scanBufferManager(), 
                                std::move(remoteRegion), 
                                *this)
        );

        auto scanDataPtr = scanData.get();
        auto res = mScans.emplace(scanId, std::move(scanData));
        if (!res.second) {
            writeErrorResponse(messageId, error::invalid_scan);
            return;
        }

        auto ec = mStorage.scan(tableId, scanDataPtr);
        if (ec) {
            writeErrorResponse(messageId, static_cast<error::errors>(ec));
            mScans.erase(res.first);
        }
    });
}

void ServerSocket::handleScanProgress(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto scanId = static_cast<uint16_t>(messageId.userId() & 0xFFFFu);

    auto offsetRead = request.read<size_t>();

    auto i = mScans.find(scanId);
    if (i == mScans.end()) {
        LOG_DEBUG("Scan progress with invalid scan ID");
        writeErrorResponse(messageId, error::invalid_scan);
        return;
    }

    i->second->requestProgress(offsetRead);
}

void ServerSocket::handleKeyTransfer(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    request.read<commitmanager::Hash>(); // rangeStart
    request.read<commitmanager::Hash>(); // rangeEnd
    request.read<uint64_t>(); // version

    handleScan(messageId, request);
}

void ServerSocket::handleRequestTransfer(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    std::unique_ptr<Transfer> transfer(new Transfer);

    transfer->start = request.read<Hash>();
    transfer->end = request.read<Hash>();
    transfer->atVersion = request.read<uint64_t>();

    LOG_INFO("Registering new key transfer @ version %1%", transfer->atVersion);
    mTransfers.push_back(std::move(transfer));
}

void ServerSocket::onWrite(uint32_t userId, uint16_t bufferId, const std::error_code& ec) {
    // TODO We have to propagate the error to the ServerScanQuery so we can detach the scan
    if (ec) {
        handleSocketError(ec);
        return;
    }

    if (bufferId != crossbow::infinio::InfinibandBuffer::INVALID_ID) {
        auto& scanBufferManager = manager().scanBufferManager();
        scanBufferManager.releaseBuffer(bufferId);
    }

    mInflightScanBuffer -= 1u;

    auto status = static_cast<uint16_t>(userId & 0xFFFFu);
    switch (status) {
    case crossbow::to_underlying(ScanStatusIndicator::ONGOING): {
        // Nothing to do
    } break;

    case crossbow::to_underlying(ScanStatusIndicator::DONE): {
        auto scanId = static_cast<uint16_t>((userId >> 16) & 0xFFFFu);
        auto i = mScans.find(scanId);
        if (i == mScans.end()) {
            LOG_ERROR("Scan progress with invalid scan ID");
            return;
        }

        LOG_DEBUG("Scan with ID %1% finished", scanId);

        i->second->completeScan();
        mScans.erase(i);

        // auto search = mTransfers.find(scanId);
        // if (search != mTransfers.end()) {
        //     // @TODO notify commit manager

        //     LOG_INFO("Key transfer %1% succeeded", scanId);
        //     mTransfers.erase(search);
        // }
    } break;

    default: {
        LOG_ERROR("Scan progress with invalid status");
    } break;
    }
}

template <typename Fun>
void ServerSocket::handleSnapshot(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& message, Fun f) {
    bool cached = (message.read<uint8_t>() != 0x0u);
    bool hasDescriptor = (message.read<uint8_t>() != 0x0u);
    message.align(sizeof(uint64_t));
    if (cached) {
        typename decltype(mSnapshots)::iterator i;
        if (!hasDescriptor) {
            // The client did not send a snapshot so it has to be in the cache
            auto version = message.read<uint64_t>();
            i = mSnapshots.find(version);
            if (i == mSnapshots.end()) {
                writeErrorResponse(messageId, error::invalid_snapshot);
                return;
            }
        } else {
            // The client send a snapshot so we have to add it to the cache (it must not already be there)
            auto snapshot = SnapshotDescriptor::deserialize(message);
            auto res = mSnapshots.emplace(snapshot->version(), std::move(snapshot));
            if (!res.second) { // Snapshot descriptor already is in the cache
                writeErrorResponse(messageId, error::invalid_snapshot);
                return;
            }
            i = res.first;
        }
        // @TODO only check after calling f?
        manager().checkTransfers(*i->second);
        f(*i->second);
    } else if (hasDescriptor) {
        auto snapshot = SnapshotDescriptor::deserialize(message);
        // @TODO only check after calling f?
        manager().checkTransfers(*snapshot);
        f(*snapshot);
    } else {
        writeErrorResponse(messageId, error::invalid_snapshot);
    }
}

void ServerSocket::removeSnapshot(uint64_t version) {
    auto i = mSnapshots.find(version);
    if (i == mSnapshots.end()) {
        return;
    }
    mSnapshots.erase(i);
}

void ServerSocket::writeModificationResponse(crossbow::infinio::MessageId messageId, int ec) {
    if (ec) {
        writeErrorResponse(messageId, static_cast<error::errors>(ec));
    } else {
        writeResponse(messageId, ResponseType::MODIFICATION, 0, []
                (crossbow::buffer_writer& /* message */, std::error_code& /* ec */) {
        });
    }
}

ServerManager::ServerManager(crossbow::infinio::InfinibandService& service,
                             Storage& storage,
                             const ServerConfig& config,
                             const ClientConfig& peersConfig)
        : Base(service, config.port),
          mToken(config.nodeToken),
          mPeersConfig(peersConfig),
          mPeersManager(peersConfig),
          mTxRunner(new MultiTransactionRunner<void>(mPeersManager)),
          mStorage(storage),
          mMaxBatchSize(config.maxBatchSize),
          mScanBufferManager(service, config),
          mMaxInflightScanBuffer(config.maxInflightScanBuffer),
          mIsShuttingDown(false) {
    
    for (decltype(config.numNetworkThreads) i = 0; i < config.numNetworkThreads; ++i) {
        mProcessors.emplace_back(service.createProcessor());
    }

    // Register with the commit-manager

    LOG_INFO("Registering with commit-manager...");

    std::unique_ptr<SnapshotDescriptor> snapshot;
    std::unique_ptr<ClusterMeta> clusterMeta;
    TransactionRunner::executeBlocking(mPeersManager, [&config, &snapshot, &clusterMeta](ClientHandle& client) {
        snapshot = std::move(client.startTransaction());
        clusterMeta = std::move(client.registerNode(*snapshot, config.nodeToken, "STORAGE"));
        client.commit(*snapshot);
    });

    // We registered successfully. This means, the commit manager should have 
    // responded with key ranges we are now responsible for. We have to
    // request these ranges from their current owners and then notify the commit manager that we now control them.
    
    std::set<crossbow::string> owners;
    for (auto range : clusterMeta->ranges) {
        if (range.owner == config.nodeToken) {
            LOG_INFO("\t-> first owner of range [%1%, %2%]", HashRing::writeHash(range.start), HashRing::writeHash(range.end));
        } else {
            LOG_INFO("\t-> request range [%1%, %2%] from %3%", HashRing::writeHash(range.start), HashRing::writeHash(range.end), range.owner);
            
            owners.insert(range.owner);

            std::unique_ptr<Transfer> transfer(new Transfer);
            transfer->start = range.start;
            transfer->end = range.end;
            transfer->atVersion = snapshot->version();

            LOG_INFO("Registering new key transfer @ version %1%", transfer->atVersion);
            mTransfers.push_back(std::move(transfer));
        }
    }

    std::vector<crossbow::infinio::Endpoint> ownerEndpoints;
    ownerEndpoints.reserve(owners.size());
    for (const auto& owner : owners) {
        ownerEndpoints.emplace_back(crossbow::infinio::Endpoint::ipv4(), owner);
    }

    mPeersConfig.tellStore = ownerEndpoints;

    if (mPeersConfig.numStores() > 0) {
        // Allocate scan memory

        size_t scanMemoryLength = 0x80000000ull;
        mScanMemory = std::move(mPeersManager.allocateScanMemory(
            mPeersConfig.numStores(),
            scanMemoryLength / mPeersConfig.numStores()
        ));

        // Re-initialize the ClientManager.
        // We have to lock the configuration, otherwise
        // the peer will reload it's own adress from the directory.
        mPeersManager.lockConfig(mPeersConfig);
        
        // Fetch the schema
        auto schemaTransferTx = std::bind(&ServerManager::transferSchema, this, _1);
        TransactionRunner::executeBlocking(mPeersManager, schemaTransferTx);
    }

    // Setup signal handling
    catchSignals();

    LOG_INFO("Storage node is ready");
}


void ServerManager::shutdown() {
    if (mIsShuttingDown.load()) {
        // Already shutting down
        return;
    }
    mIsShuttingDown.store(true);

    LOG_INFO("Shutting down storage node...");

    // Unregister with the commit-manager

    LOG_INFO("Unregistering with commit-manager...");

    std::unique_ptr<SnapshotDescriptor> snapshot;
    std::unique_ptr<ClusterMeta> clusterMeta;
    TransactionRunner::executeBlocking(mPeersManager, [this, &snapshot, &clusterMeta](ClientHandle& client) {
        snapshot = std::move(client.startTransaction());
        // clusterMeta = std::move(client.unregisterNode(*snapshot));
        clusterMeta = std::move(client.unregisterNode(*snapshot, mToken));
        client.commit(*snapshot);
    });

    // We unregistered successfully. This means, the commit manager should have 
    // responded with new owners for the key ranges we are giving up.
    // We will notify those owners, so that they can request the ranges from us.

    std::set<crossbow::string> owners;
    for (const auto& range : clusterMeta->ranges) {
        LOG_INFO("\t[%1%, %2%] -> %3%", HashRing::writeHash(range.start), HashRing::writeHash(range.end), range.owner);
        owners.insert(range.owner);
    }

    std::vector<crossbow::infinio::Endpoint> ownerEndpoints;
    ownerEndpoints.reserve(owners.size());
    for (const auto& owner : owners) {
        ownerEndpoints.emplace_back(crossbow::infinio::Endpoint::ipv4(), owner);
    }

    // Create and load new configuration
    ClientConfig config(mPeersConfig);
    config.tellStore = ownerEndpoints;

    mPeersManager.lockConfig(config);

    for (const auto& range : clusterMeta->ranges) {
        auto tx = std::bind(&ServerManager::requestTransfer, this, range.owner, range.start, range.end, snapshot->version(), _1);
        mTxRunner->execute(tx);
    }
    
    // Base::shutdown();
    // std::exit(0);
}


ServerSocket* ServerManager::createConnection(crossbow::infinio::InfinibandSocket socket,
        const crossbow::string& data) {
    // The length of the data field may be larger than the actual data sent (behavior of librdma_cm) so check the
    // handshake against the substring
    auto& handshake = handshakeString();
    if (data.size() < (handshake.size() + sizeof(uint64_t)) || data.substr(0, handshake.size()) != handshake) {
        LOG_ERROR("Connection handshake failed");
        return nullptr;
    }
    auto thread = *reinterpret_cast<const uint64_t*>(&data[handshake.size()]);
    auto& processor = *mProcessors.at(thread % mProcessors.size());

    LOG_INFO("%1%] New client connection on processor %2%", socket->remoteAddress(), thread);
    return new ServerSocket(
        *this,
        mStorage, 
        processor, 
        std::move(socket), 
        mMaxBatchSize, 
        mMaxInflightScanBuffer
    );
}

void ServerManager::checkTransfer(Transfer& transfer, const SnapshotDescriptor& snapshot) {
    if (!transfer.started) {
        if (snapshot.lowestActiveVersion() >= transfer.atVersion) {
            LOG_INFO("Starting key transfer...");
            
            transfer.started = true;
            performTransfer(transfer);
        }
    }
}

void ServerManager::checkTransfers(const SnapshotDescriptor& snapshot) {
    // Check the server managers own queued transfers
    for (const auto& transfer : mTransfers) {
        checkTransfer(*transfer, snapshot);
    }

    // Check each sockets queued transfers
    for (const auto& socket : mSockets) {
        for (const auto& transfer : socket->mTransfers) {
            checkTransfer(*transfer, snapshot);
        }
    }
}

const uint32_t SELECTION_LENGTH = 136;
/**
 * Constructs a scan query that matches a specific key range.
 */
std::unique_ptr<char[]> createKeyTransferQuery(Table& table, Hash rangeStart, Hash rangeEnd) {
    std::unique_ptr<char[]> selection(new char[SELECTION_LENGTH]);

    crossbow::buffer_writer selectionWriter(selection.get(), SELECTION_LENGTH);
    selectionWriter.write<uint32_t>(0x1u);      // Number of columns
    selectionWriter.write<uint16_t>(0x2u);      // Number of conjuncts
    selectionWriter.write<uint16_t>(0x0u);      // Partition shift
    selectionWriter.write<uint32_t>(0x0u);      // Partition key
    selectionWriter.write<uint32_t>(0x0u);      // Partition value
    
    // Selection on the partition key
    Record::id_t keyField;
    bool hasPartKeyField = table.record().idOf("__partition_token", keyField);
    if (hasPartKeyField == false) {
        LOG_ERROR("Schema must have a partition key field.");
    }

    selectionWriter.write<uint16_t>(keyField);      // -1 is the fixed id of the tell key field
    selectionWriter.write<uint16_t>(0x2u);          // number of predicates
    selectionWriter.align(sizeof(uint64_t));        
    
    // We need a predicate P(key) := rangeStart < key <= rangeEnd
    
    // Start with rangeStart < key
    selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::UNSIGNED_GREATER));
    selectionWriter.write<uint8_t>(0x0u);           // predicate id
    selectionWriter.align(sizeof(uint64_t));
    selectionWriter.write<Hash>(rangeStart);
    
    // And then key <= rangeEnd
    selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::UNSIGNED_LESS_EQUAL));
    selectionWriter.write<uint8_t>(0x1u);           // predicate id
    selectionWriter.align(sizeof(uint64_t));
    selectionWriter.write<Hash>(rangeEnd);

    return selection;
}

void ServerManager::transferSchema(ClientHandle& client) {
    LOG_INFO("Fetching schema...");
    auto tablesFuture = client.getTables();
    if (auto ec = tablesFuture->error()) {
        LOG_ERROR("Error fetching remote tables [error = %1% %2%]", ec, ec.message());
    }

    auto tables = tablesFuture->get();

    LOG_INFO("Creating %1% local tables...", tables.size());
    for (const auto& table : tables) {
        LOG_INFO("\t %1% (%2%, %3%)", table.tableId(), table.tableName(), table.tableType() == TableType::NON_TRANSACTIONAL);
        
        // It is important to create the local tables with id's
        // matching their remotes counterpart, as client's assume
        // that table id's match across the cluster.
        auto succeeded = mStorage.createTable(table.tableId(),
                                              table.tableName(), 
                                              table.record().schema());
        if (!succeeded) {
            LOG_ERROR("\tCould not create table %1%", table.tableName());
        }            
    }
}

void ServerManager::requestTransfer(const crossbow::string& host, 
                                    Hash rangeStart, 
                                    Hash rangeEnd, 
                                    uint64_t version, 
                                    ClientHandle& client) {
    client.requestTransfer(host, rangeStart, rangeEnd, version);
}

void ServerManager::transferKeys(crossbow::string tableName, const Transfer& transfer, ClientHandle& client) {
    auto snapshot = client.startTransaction(TransactionType::READ_ONLY);

    auto table = client.getTable(tableName)->get();

    auto transferQuery = createKeyTransferQuery(table, transfer.start, transfer.end);

    auto transferIterator = client.transferKeys(
        transfer.start,
        transfer.end,
        table,
        *snapshot,
        *mScanMemory,
        ScanQueryType::FULL,
        SELECTION_LENGTH,
        transferQuery.get(),
        0x0u,
        nullptr
    );

    uint64_t localTableId = 0x0u;
    mStorage.getTable(tableName, localTableId);

    size_t errorCount = 0x0u;
    size_t scanCount = 0x0u;
    while (transferIterator->hasNext()) {
        uint64_t key;
        uint64_t validFrom;
        uint64_t validTo;
        const char* tuple;
        size_t tupleLength;
        std::tie(key, validFrom, validTo, tuple, tupleLength) = transferIterator->next();
        ++scanCount;
        
        // @TODO As discussed in the thesis, this is kind of a hack.
        // We construct a SnapshotDescriptor that matches the incoming tuple's versioning information.
        // Will have to be extended to copy the tuple's entire active versioning history.

        commitmanager::SnapshotDescriptor::BlockType descriptor = 0x0u;
        auto snapshot = commitmanager::SnapshotDescriptor::create(0x0u, validFrom-1, validFrom, reinterpret_cast<const char*>(&descriptor));
        auto ec = mStorage.insert(localTableId, key, tupleLength, tuple, *snapshot);
        
        // @TODO Ignore errors due to write collisions, simply discard the incoming write
        // This will be reconsidered anyways when implementing a proper merge.
        if (ec != 0) {
            LOG_ERROR("\tInsertion failed with error code %1%", ec);
            ++errorCount;
        }
    }

    if (transferIterator->error()) {
        auto& ec = transferIterator->error();
        LOG_ERROR("Error scanning table [error = %1% %2%]", ec, ec.message());
    } else if (errorCount > 0) {
        LOG_ERROR("There were errors.");
        // std::exit(1);
    }

    client.commit(*snapshot);

    if (scanCount > 0 || errorCount > 0) {
        LOG_INFO("\t\t received %1% tuples in total, %2% errors", scanCount, errorCount);
    }
}

void ServerManager::transferOwnership(Hash rangeStart, Hash rangeEnd, ClientHandle& client) {
    auto snapshot = client.startTransaction(TransactionType::READ_ONLY);

    client.transferOwnership(rangeEnd, mToken);

    client.commit(*snapshot);
}

void ServerManager::performTransfer(const Transfer& transfer) {
    for (const auto& table : mStorage.getTables()) {
        LOG_INFO("\t-> on table %1% (%2%)", table->tableId(), table->tableName());
        auto tx = std::bind(&ServerManager::transferKeys, this, table->tableName(), transfer, _1);
        
        // @TODO: This should use the MultiTransactionRunner
        // Currently this causes some problems with the allocated scan memory.
        // mTxRunner->execute(tx);
        
        TransactionRunner::executeBlocking(mPeersManager, tx);
    }

    auto ack = std::bind(&ServerManager::transferOwnership, this, transfer.start, transfer.end, _1);
    TransactionRunner::executeBlocking(mPeersManager, ack);
}


} // namespace store
} // namespace tell
