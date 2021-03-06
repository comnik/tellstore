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
#pragma once

#include <commitmanager/MessageTypes.hpp>

#include <tellstore/ClientConfig.hpp>
#include <tellstore/ErrorCode.hpp>
#include <tellstore/MessageTypes.hpp>
#include <tellstore/GenericTuple.hpp>
#include <tellstore/Record.hpp>
#include <tellstore/ScanMemory.hpp>
#include <tellstore/Table.hpp>

#include <crossbow/byte_buffer.hpp>
#include <crossbow/infinio/InfinibandSocket.hpp>
#include <crossbow/infinio/RpcClient.hpp>
#include <crossbow/string.hpp>

#include <sparsehash/dense_hash_map>

#include <cstdint>
#include <memory>
#include <system_error>
#include <tuple>

namespace tell {
namespace commitmanager {
class SnapshotDescriptor;
} // namespace commitmanager

namespace store {

class AbstractTuple;
class ClientSocket;
class ScanIterator;

/**
* @brief Response wrapper that encpasulates a possible cluster status request,
*        if the local cluster configuration is missing.
*/
template <class ResponseType>
class ClusterResponse final {
public:
    // Fast-path constructor
    ClusterResponse( std::shared_ptr<ResponseType> resp ) 
        : mResponse(resp) {}
    
    // Constructor with retry
    ClusterResponse( std::shared_ptr<ResponseType> resp,
                     std::shared_ptr<ResponseType> retryResp ) 
        : mResponse(resp),
          mRetryResponse(retryResp) {}

    std::shared_ptr<ResponseType> get() {
        if (mRetryResponse == nullptr) {
            // No retry set
            return mResponse;
        } else {
            // First we have to wait for the original request to finish and see if it fails
            bool destSucceeded = mResponse->waitForResult();
            if (destSucceeded) {
                return mResponse;                
            } else {
                auto& ec = mResponse->error();
                LOG_INFO("ClusterResponse caught error %1% (%2%)", ec, ec.message());

                bool srcSucceeded = mRetryResponse->waitForResult();
                if (!srcSucceeded) {
                    auto& ec = mRetryResponse->error();
                    LOG_INFO("Retry response failed with error %1% (%2%)", ec, ec.message());
                }

                return mRetryResponse;
            }            
        }
    }

    /** Will only return a successful response if both requests succeeded */
    std::shared_ptr<ResponseType> getQuorum() {
        if (mRetryResponse == nullptr) {
            // No retry set
            return mResponse;
        } else {
            bool destSucceeded = mResponse->waitForResult();
            bool srcSucceeded = mRetryResponse->waitForResult();

            if (!destSucceeded) {
                auto& ec = mResponse->error();
                LOG_INFO("Replicated write failed at new owner with %1% (%2%)", ec, ec.message());
                return mResponse;
            } else if (!srcSucceeded) {
                auto& ec = mRetryResponse->error();
                LOG_INFO("Replicated write failed at old owner with %1% (%2%)", ec, ec.message());
                return mRetryResponse;
            }
            
            return mResponse;
        }
    }
    
private:
    std::shared_ptr<ResponseType> mResponse;
    std::shared_ptr<ResponseType> mRetryResponse;
};

/**
 * @brief Response for a Create-Table request
 */
class CreateTableResponse final
        : public crossbow::infinio::RpcResponseResult<CreateTableResponse, uint64_t> {
    using Base = crossbow::infinio::RpcResponseResult<CreateTableResponse, uint64_t>;

public:
    CreateTableResponse(crossbow::infinio::Fiber& fiber)
            : Base(fiber) {
    }

private:
    friend Base;

    static constexpr ResponseType MessageType = ResponseType::CREATE_TABLE;

    static const std::error_category& errorCategory() {
        return error::get_error_category();
    }

    void processResponse(crossbow::buffer_reader& message);
};

/**
 * @brief Response for a Get-Tables request
 */
class GetTablesResponse final : public crossbow::infinio::RpcResponseResult<GetTablesResponse, std::vector<Table>> {
    using Base = crossbow::infinio::RpcResponseResult<GetTablesResponse, std::vector<Table>>;

public:
    using Base::Base;

private:
    friend Base;

    static constexpr ResponseType MessageType = ResponseType::GET_TABLES;

    static const std::error_category& errorCategory() {
        return error::get_error_category();
    }

    void processResponse(crossbow::buffer_reader& message);
};

/**
 * @brief Response for a Get-Table request
 */
class GetTableResponse final : public crossbow::infinio::RpcResponseResult<GetTableResponse, Table> {
    using Base = crossbow::infinio::RpcResponseResult<GetTableResponse, Table>;

public:
    GetTableResponse(crossbow::infinio::Fiber& fiber, const crossbow::string& tableName)
            : Base(fiber),
              mTableName(tableName) {
    }

private:
    friend Base;

    static constexpr ResponseType MessageType = ResponseType::GET_TABLE;

    static const std::error_category& errorCategory() {
        return error::get_error_category();
    }

    void processResponse(crossbow::buffer_reader& message);

    crossbow::string mTableName;
};

/**
 * @brief Response for a Get request
 */
class GetResponse final : public crossbow::infinio::RpcResponseResult<GetResponse, std::unique_ptr<Tuple>> {
    using Base = crossbow::infinio::RpcResponseResult<GetResponse, std::unique_ptr<Tuple>>;

public:
    using Base::Base;

private:
    friend Base;

    static constexpr ResponseType MessageType = ResponseType::GET;

    static const std::error_category& errorCategory() {
        return error::get_error_category();
    }

    void processResponse(crossbow::buffer_reader& message);
};


/**
 * @brief Response for a Modification (insert, update, remove, revert) request
 */
class ModificationResponse final : public crossbow::infinio::RpcResponseResult<ModificationResponse, void> {
    using Base = crossbow::infinio::RpcResponseResult<ModificationResponse, void>;

public:
    using Base::Base;

private:
    friend Base;

    static constexpr ResponseType MessageType = ResponseType::MODIFICATION;

    static const std::error_category& errorCategory() {
        return error::get_error_category();
    }

    void processResponse(crossbow::buffer_reader& message);
};

/**
 * @brief Response for a Scan request
 */
class ScanResponse final : public crossbow::infinio::RpcResponse, public std::enable_shared_from_this<ScanResponse> {
public:
    ScanResponse(crossbow::infinio::Fiber& fiber, std::shared_ptr<ScanIterator> iterator, ClientSocket& socket,
            ScanMemory memory, uint16_t scanId);

    const ScanMemory& scanMemory() const {
        return mMemory;
    }

    /**
     * @brief Whether the remote server has written any new data
     */
    bool available() const {
        return mOffsetWritten > mOffsetRead;
    }

    /**
     * @brief Returns the next available data chunk written by the remote server
     *
     * @return Tuple containing the start and end pointer to the next available chunk
     */
    std::tuple<const char*, const char*> nextChunk();

private:
    friend class ClientSocket;

    virtual void onResponse(uint32_t messageType, crossbow::buffer_reader& message) final override;

    virtual void onAbort(std::error_code ec) final override;

    std::weak_ptr<ScanIterator> mIterator;

    ClientSocket& mSocket;

    /// Memory region containing the data written by the remote server
    ScanMemory mMemory;

    /// ID of the scan in the remote server
    uint16_t mScanId;

    /// Amount of data read by the client
    size_t mOffsetRead;

    /// Amount of data written by the remote server
    size_t mOffsetWritten;
};

/**
 * @brief Iterator encapsulating the ScanResponse objects across all shards
 */
class ScanIterator {
public:
    ScanIterator(crossbow::infinio::Fiber& fiber, Record record, size_t shardSize);

    const Record& record() const {
        return mRecord;
    }

    const std::error_code& error() const {
        return mError;
    }

    /**
     * @brief Whether the scan has pending elements to read
     *
     * Blocks until the scan is done or the next element is available.
     */
    bool hasNext();

    /**
     * @brief Advances the iterator to the next position and returns the element data
     *
     * @return Tuple containing the key, pointer to the data and the size of the element
     */
    std::tuple<uint64_t, const char*, size_t> next();

    /**
     * @brief Returns the current chunk of elements and advances the iterator to the next chunk
     *
     * @return Tuple containing the start and end pointer to the current chunk
     */
    std::tuple<const char*, const char*> nextChunk();

    void wait();

private:
    friend class BaseClientProcessor;
    friend class ScanResponse;
    friend class TransferIterator;

    void addScanResponse(std::shared_ptr<ScanResponse> response) {
        mScans.emplace_back(std::move(response));
    }

    /**
     * @brief Notify the iterator about new data in any ScanResponse
     */
    void notify();

    /**
     * @brief Notify the iterator about an error in a ScanResponse
     */
    void abort(std::error_code ec);

    crossbow::infinio::Fiber& mFiber;

    std::vector<std::shared_ptr<ScanResponse>> mScans;

    Record mRecord;

    bool mWaiting;

    std::error_code mError;

    const char* mChunkPos;

    const char* mChunkEnd;
};

/**
 * @brief Extends ScanIterator to return full tuple information
 * as used during a key transfer.
 */
class TransferIterator : public ScanIterator {
public:
    TransferIterator(crossbow::infinio::Fiber& fiber, Record record, size_t shardSize) 
            : ScanIterator(fiber, record, shardSize) {};

    /** Returns a tuple (key, validFrom, validTo, data, length) */
    std::tuple<uint64_t, uint64_t, uint64_t, const char*, size_t> next();
};

/**
 * @brief Handles communication with one TellStore server
 *
 * Sends RPC requests and returns the pending response.
 */
class ClientSocket final : public crossbow::infinio::RpcClientSocket {
    using Base = crossbow::infinio::RpcClientSocket;
public:
    using Base::Base;

    void connect(const crossbow::infinio::Endpoint& host, uint64_t threadNum);

    void shutdown();

    std::shared_ptr<CreateTableResponse> createTable(crossbow::infinio::Fiber& fiber, const crossbow::string& name,
            const Schema& schema);

    std::shared_ptr<GetTablesResponse> getTables(crossbow::infinio::Fiber& fiber);

    std::shared_ptr<GetTableResponse> getTable(crossbow::infinio::Fiber& fiber, const crossbow::string& name);

    std::shared_ptr<GetResponse> get(   crossbow::infinio::Fiber& fiber,
                                        uint64_t tableId,
                                        uint64_t key,
                                        const commitmanager::SnapshotDescriptor& snapshot );

    std::shared_ptr<ModificationResponse> insert(   crossbow::infinio::Fiber& fiber,
                                                    uint64_t tableId,
                                                    uint64_t key,
                                                    const commitmanager::SnapshotDescriptor& snapshot,
                                                    AbstractTuple& tuple );

    std::shared_ptr<ModificationResponse> update(   crossbow::infinio::Fiber& fiber,
                                                    uint64_t tableId,
                                                    uint64_t key,
                                                    const commitmanager::SnapshotDescriptor& snapshot,
                                                    AbstractTuple& tuple );

    std::shared_ptr<ModificationResponse> remove(   crossbow::infinio::Fiber& fiber,
                                                    uint64_t tableId,
                                                    uint64_t key,
                                                    const commitmanager::SnapshotDescriptor& snapshot );

    std::shared_ptr<ModificationResponse> revert(   crossbow::infinio::Fiber& fiber, 
                                                    uint64_t tableId, 
                                                    uint64_t key,
                                                    const commitmanager::SnapshotDescriptor& snapshot );

    void scanStart( uint16_t scanId,
                    std::shared_ptr<ScanResponse> response,
                    uint64_t tableId,
                    ScanQueryType queryType,
                    uint32_t selectionLength, 
                    const char* selection, 
                    uint32_t queryLength, 
                    const char* query,
                    const commitmanager::SnapshotDescriptor& snapshot );

    void scanProgress(uint16_t scanId, std::shared_ptr<ScanResponse> response, size_t offset);

    void scanComplete(uint16_t scanId) {
        completeAsyncRequest(scanId);
    }

    std::shared_ptr<ModificationResponse> requestTransfer(  crossbow::infinio::Fiber& fiber,
                                                            commitmanager::Hash rangeStart,
                                                            commitmanager::Hash rangeEnd,
                                                            uint64_t version );

    void transferKeys(  commitmanager::Hash rangeStart,
                        commitmanager::Hash rangeEnd,
                        uint16_t scanId,
                        std::shared_ptr<ScanResponse> response,
                        uint64_t tableId,
                        ScanQueryType queryType,
                        uint32_t selectionLength, 
                        const char* selection, 
                        uint32_t queryLength, 
                        const char* query,
                        const commitmanager::SnapshotDescriptor& snapshot );
};

extern template class ClusterResponse<GetResponse>;
extern template class ClusterResponse<ModificationResponse>;

} // namespace store
} // namespace tell
