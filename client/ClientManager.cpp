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
#include <commitmanager/HashRing.hpp>

#include <tellstore/ClientManager.hpp>

namespace tell {
namespace store {
namespace {

void checkTableType(const Table& table, TableType type) {
    if (table.tableType() != type) {
        throw std::logic_error("Operation not supported on table");
    }
}

} // anonymous namespace

std::unique_ptr<commitmanager::SnapshotDescriptor> ClientHandle::createNonTransactionalSnapshot(uint64_t baseVersion) {
    auto version = (baseVersion == std::numeric_limits<uint64_t>::max() ? baseVersion : baseVersion + 1);
    commitmanager::SnapshotDescriptor::BlockType descriptor = 0x0u;
    
    auto snapshot = commitmanager::SnapshotDescriptor::create(0x0u, baseVersion, version, reinterpret_cast<const char*>(&descriptor));
    snapshot->nodeRing = std::unique_ptr<commitmanager::HashRing>(new commitmanager::HashRing(1));
    snapshot->nodeRing->insertNode("0.0.0.0:7243");
    snapshot->nodeRing->insertNode("0.0.0.0:7244");

    return snapshot;
}

std::unique_ptr<commitmanager::SnapshotDescriptor> ClientHandle::createAnalyticalSnapshot(uint64_t lowestActiveVersion,
        uint64_t baseVersion) {
    auto snapshot = commitmanager::SnapshotDescriptor::create(lowestActiveVersion, baseVersion, baseVersion, nullptr);
    snapshot->nodeRing = std::unique_ptr<commitmanager::HashRing>(new commitmanager::HashRing(1));
    snapshot->nodeRing->insertNode("0.0.0.0:7243");
    snapshot->nodeRing->insertNode("0.0.0.0:7244");

    return snapshot;    
}

std::unique_ptr<commitmanager::ClusterMeta> ClientHandle::registerNode(const commitmanager::SnapshotDescriptor& snapshot,
                                                                       crossbow::string host, 
                                                                       crossbow::string tag) {
    return mProcessor.registerNode(mFiber, snapshot, host, tag);
}

std::unique_ptr<commitmanager::ClusterMeta> ClientHandle::unregisterNode(const commitmanager::SnapshotDescriptor& snapshot,
                                                                         crossbow::string host) {
    return mProcessor.unregisterNode(mFiber, snapshot, host);
}

void ClientHandle::transferOwnership(commitmanager::Hash rangeEnd, crossbow::string host) {
    return mProcessor.transferOwnership(mFiber, rangeEnd, host);
}

std::unique_ptr<commitmanager::SnapshotDescriptor> ClientHandle::startTransaction(
        TransactionType type /* = TransactionType::READ_WRITE */) {
    auto snapshot = mProcessor.start(mFiber, type);

    // mOldPartitioning.store(new HashRing_t(*snapshot->nodeRing), std::memory_order_relaxed);

    return snapshot;
}

void ClientHandle::commit(const commitmanager::SnapshotDescriptor& snapshot) {
    mProcessor.commit(mFiber, snapshot);
}

Table ClientHandle::createTable(const crossbow::string& name, Schema schema) {
    return mProcessor.createTable(mFiber, name, std::move(schema));
}

std::shared_ptr<GetTablesResponse> ClientHandle::getTables() {
    return mProcessor.getTables(mFiber);
}

std::shared_ptr<GetTableResponse> ClientHandle::getTable(const crossbow::string& name) {
    return mProcessor.getTable(mFiber, name);
}

std::shared_ptr<GetResponse> ClientHandle::get(const Table& table, uint64_t key) {
    checkTableType(table, TableType::NON_TRANSACTIONAL);

    auto snapshot = createNonTransactionalSnapshot(std::numeric_limits<uint64_t>::max());

    auto clusterResp = mProcessor.get(mFiber, table.tableId(), key, *snapshot);
    return clusterResp->get();
}

std::shared_ptr<GetResponse> ClientHandle::get(const Table& table, 
                                               uint64_t key,
                                               const commitmanager::SnapshotDescriptor& snapshot) {
    checkTableType(table, TableType::TRANSACTIONAL);
    
    auto clusterResp = mProcessor.get(mFiber, table.tableId(), key, snapshot);
    return clusterResp->get();
}

std::shared_ptr<ModificationResponse> ClientHandle::insert(const Table& table, 
                                                           uint64_t key, 
                                                           uint64_t version,
                                                           GenericTuple data) {
    GenericTupleSerializer tuple(table.record(), std::move(data));
    return insert(table, key, version, tuple);
}

std::shared_ptr<ModificationResponse> ClientHandle::insert(const Table& table, 
                                                           uint64_t key, 
                                                           uint64_t version,
                                                           AbstractTuple& tuple) {
    checkTableType(table, TableType::NON_TRANSACTIONAL);

    auto snapshot = createNonTransactionalSnapshot(version);

    auto clusterResp = mProcessor.insert(mFiber, table.tableId(), key, *snapshot, tuple);
    return clusterResp->getQuorum();
}

std::shared_ptr<ModificationResponse> ClientHandle::insert(const Table& table, 
                                                           uint64_t key, 
                                                           const commitmanager::SnapshotDescriptor& snapshot, 
                                                           GenericTuple data) {
    GenericTupleSerializer tuple(table.record(), std::move(data));
    return insert(table, key, snapshot, tuple);
}

std::shared_ptr<ModificationResponse> ClientHandle::insert(const Table& table, 
                                                           uint64_t key, 
                                                           const commitmanager::SnapshotDescriptor& snapshot, 
                                                           AbstractTuple& tuple) {
    checkTableType(table, TableType::TRANSACTIONAL);

    auto clusterResp = mProcessor.insert(mFiber, table.tableId(), key, snapshot, tuple);
    return clusterResp->getQuorum();
}

std::shared_ptr<ModificationResponse> ClientHandle::update(const Table& table, 
                                                           uint64_t key, 
                                                           uint64_t version,
                                                           GenericTuple data) {
    GenericTupleSerializer tuple(table.record(), std::move(data));
    return update(table, key, version, tuple);
}

std::shared_ptr<ModificationResponse> ClientHandle::update(const Table& table, 
                                                           uint64_t key, 
                                                           uint64_t version, 
                                                           AbstractTuple& tuple) {
    checkTableType(table, TableType::NON_TRANSACTIONAL);

    auto snapshot = createNonTransactionalSnapshot(version);

    auto clusterResp = mProcessor.update(mFiber, table.tableId(), key, *snapshot, tuple);
    return clusterResp->getQuorum();
}

std::shared_ptr<ModificationResponse> ClientHandle::update(const Table& table, uint64_t key,
        const commitmanager::SnapshotDescriptor& snapshot, GenericTuple data) {
    GenericTupleSerializer tuple(table.record(), std::move(data));
    return update(table, key, snapshot, tuple);
}

std::shared_ptr<ModificationResponse> ClientHandle::update(const Table& table, 
                                                           uint64_t key, 
                                                           const commitmanager::SnapshotDescriptor& snapshot, 
                                                           AbstractTuple& tuple) {
    checkTableType(table, TableType::TRANSACTIONAL);
    
    auto clusterResp = mProcessor.update(mFiber, table.tableId(), key, snapshot, tuple);
    return clusterResp->getQuorum();
}

std::shared_ptr<ModificationResponse> ClientHandle::remove(const Table& table, 
                                                           uint64_t key, 
                                                           uint64_t version) {
    checkTableType(table, TableType::NON_TRANSACTIONAL);

    auto snapshot = createNonTransactionalSnapshot(version);

    auto clusterResp = mProcessor.remove(mFiber, table.tableId(), key, *snapshot);
    return clusterResp->getQuorum();
}

std::shared_ptr<ModificationResponse> ClientHandle::remove(const Table& table, 
                                                           uint64_t key, 
                                                           const commitmanager::SnapshotDescriptor& snapshot) {
    checkTableType(table, TableType::TRANSACTIONAL);
    
    auto clusterResp = mProcessor.remove(mFiber, table.tableId(), key, snapshot);
    return clusterResp->getQuorum();
}

std::shared_ptr<ModificationResponse> ClientHandle::revert(const Table& table, 
                                                           uint64_t key, 
                                                           const commitmanager::SnapshotDescriptor& snapshot) {
    checkTableType(table, TableType::TRANSACTIONAL);
    
    auto clusterResp = mProcessor.revert(mFiber, table.tableId(), key, snapshot);
    return clusterResp->getQuorum();
}

std::shared_ptr<ScanIterator> ClientHandle::scan(const Table& table,
                                                 const commitmanager::SnapshotDescriptor& snapshot,
                                                 ScanMemoryManager& memoryManager,
                                                 ScanQueryType queryType,
                                                 uint32_t selectionLength, 
                                                 const char* selection,
                                                 uint32_t queryLength,
                                                 const char* query) {
    checkTableType(table, TableType::TRANSACTIONAL);

    return mProcessor.scan(
        mFiber,
        table.tableId(),
        snapshot, 
        table.record(), 
        memoryManager, 
        queryType, 
        selectionLength,
        selection, 
        queryLength, 
        query
    );
}

std::shared_ptr<TransferIterator> ClientHandle::transferKeys(commitmanager::Hash rangeStart,
                                                             commitmanager::Hash rangeEnd,
                                                             const Table& table,
                                                             const commitmanager::SnapshotDescriptor& snapshot,
                                                             ScanMemoryManager& memoryManager,
                                                             ScanQueryType queryType, 
                                                             uint32_t selectionLength, 
                                                             const char* selection,
                                                             uint32_t queryLength, 
                                                             const char* query) {
    return mProcessor.transferKeys(
        mFiber,
        rangeStart,
        rangeEnd,
        table.tableId(),
        snapshot, 
        table.record(), 
        memoryManager, 
        queryType, 
        selectionLength,
        selection, 
        queryLength, 
        query
    );
}

std::shared_ptr<ModificationResponse> ClientHandle::requestTransfer(const crossbow::string& host,
                                                                    commitmanager::Hash rangeStart,
                                                                    commitmanager::Hash rangeEnd,
                                                                    uint64_t version) {
    return mProcessor.requestTransfer(mFiber, host, rangeStart, rangeEnd, version);
}

BaseClientProcessor::BaseClientProcessor(crossbow::infinio::InfinibandService& service,
                                         const ClientConfig& config,
                                         uint64_t processorNum) 
    : mConfig(config),
      mService(service),
      mCachedDirectoryVersion(0),
      mProcessor(service.createProcessor()),
      mCommitManagerSocket(service.createSocket(*mProcessor), config.maxPendingResponses, config.maxBatchSize),
      mProcessorNum(processorNum),
      mScanId(0u) {

    // Load the configuration initially
    reloadConfig(config);
}

void BaseClientProcessor::reloadConfig(const ClientConfig& config) {
    LOG_DEBUG("[PROC %1%] Reloading processor config...", mProcessorNum);

    if (!mCommitManagerSocket.isConnected()) {
        mCommitManagerSocket.connect(config.commitManager);
    }

    // Only connections to new hosts will be created,
    // existing ones will be kept.

    for (auto& ep : config.getStores()) {
        auto search = mTellStoreSocket.find(ep.getToken());
        if (search == mTellStoreSocket.end()) {
            // Socket not yet contained
            std::unique_ptr<store::ClientSocket> socket(new ClientSocket(
                mService.createSocket(*mProcessor),
                config.maxPendingResponses,
                config.maxBatchSize
            ));

            socket->connect(ep, mProcessorNum);
            
            mTellStoreSocket[ep.getToken()] = std::move(socket);
        }
    }
    
    LOG_DEBUG("[PROC %1%] Connected to %2% hosts", mProcessorNum, mTellStoreSocket.size());
}

void BaseClientProcessor::shutdown() {
    if (mProcessor->threadId() == std::this_thread::get_id()) {
        throw std::runtime_error("Unable to shutdown from within the processing thread");
    }

    if (mCommitManagerSocket.isConnected()) {
        mCommitManagerSocket.shutdown();
    }

    for (auto& socketIt : mTellStoreSocket) {
        socketIt.second->shutdown();
    }
}

std::unique_ptr<commitmanager::ClusterMeta> BaseClientProcessor::registerNode(crossbow::infinio::Fiber& fiber, 
                                                                              const commitmanager::SnapshotDescriptor& snapshot,
                                                                              crossbow::string host, 
                                                                              crossbow::string tag) {
    auto registerResponse = mCommitManagerSocket.registerNode(fiber, snapshot, host, tag);
    if (auto& ec = registerResponse->error()) {
        LOG_ERROR("Error while registering [error = %1% %2%]", ec, ec.message());
    }
    return registerResponse->get();
}

std::unique_ptr<commitmanager::ClusterMeta> BaseClientProcessor::unregisterNode(crossbow::infinio::Fiber& fiber, 
                                                                                const commitmanager::SnapshotDescriptor& snapshot,
                                                                                crossbow::string host) {
    auto unregisterResponse = mCommitManagerSocket.unregisterNode(fiber, snapshot, host);
    if (auto& ec = unregisterResponse->error()) {
        LOG_ERROR("Error while unregistering [error = %1% %2%]", ec, ec.message());
    }
    return unregisterResponse->get();
}

void BaseClientProcessor::transferOwnership(crossbow::infinio::Fiber& fiber,
                                            commitmanager::Hash rangeEnd,
                                            crossbow::string host) {
    auto resp = mCommitManagerSocket.transferOwnership(fiber, rangeEnd, host);
    if (auto& ec = resp->error()) {
        LOG_ERROR("Error while transfering ownership [error = %1% %2%]", ec, ec.message());
    }
    resp->get();
}

std::unique_ptr<commitmanager::SnapshotDescriptor> BaseClientProcessor::start(crossbow::infinio::Fiber& fiber, 
                                                                        TransactionType type) {
    // TODO Return a transaction future?

    auto startResponse = mCommitManagerSocket.startTransaction(fiber, type != TransactionType::READ_WRITE);
    auto snapshot = startResponse->get();

    if (snapshot->directoryVersion > mCachedDirectoryVersion && !mConfig.isLocked) {
        LOG_INFO("Updating directory @ %1% (cached is %2%, peers: %3%)", snapshot->directoryVersion, mCachedDirectoryVersion, snapshot->peers);
    
        // We are the first thread to update the partition information
        auto endpoints = ClientConfig::parseTellStore(snapshot->peers);

        // Create and load new configuration
        ClientConfig config(mConfig);
        config.tellStore = endpoints;

        snapshot->numPeers = config.numStores();
        
        reloadConfig(config);

        // Update thread-local routing information
        mCachedDirectoryVersion = snapshot->directoryVersion;
    }

    return snapshot;
}

void BaseClientProcessor::commit(crossbow::infinio::Fiber& fiber, const commitmanager::SnapshotDescriptor& snapshot) {
    // TODO Return a commit future?

    auto commitResponse = mCommitManagerSocket.commitTransaction(fiber, snapshot.version());
    if (!commitResponse->get()) {
        throw std::runtime_error("Commit transaction did not succeed");
    }
}

Table BaseClientProcessor::createTable(crossbow::infinio::Fiber& fiber, const crossbow::string& name, Schema schema) {
    // TODO Return a combined createTable future?
    std::vector<std::shared_ptr<CreateTableResponse>> requests;
    requests.reserve(mTellStoreSocket.size());
    for (auto& socketIt : mTellStoreSocket) {
        requests.emplace_back(socketIt.second->createTable(fiber, name, schema));
    }
    uint64_t tableId = 0u;
    for (auto& i : requests) {
        auto id = i->get();
        LOG_ASSERT(tableId == 0u || tableId == id, "Table IDs returned from shards do not match");
        tableId = id;
    }
    return Table(tableId, name, std::move(schema));
}

std::shared_ptr<ScanIterator> BaseClientProcessor::scan(crossbow::infinio::Fiber& fiber, 
                                                        uint64_t tableId,
                                                        const commitmanager::SnapshotDescriptor& snapshot, 
                                                        Record record, 
                                                        ScanMemoryManager& memoryManager,
                                                        ScanQueryType queryType, 
                                                        uint32_t selectionLength, 
                                                        const char* selection, 
                                                        uint32_t queryLength,
                                                        const char* query) {
    auto scanId = ++mScanId;

    auto iterator = std::make_shared<ScanIterator>(fiber, std::move(record), mTellStoreSocket.size());
    for (auto& socketIt : mTellStoreSocket) {
        auto memory = memoryManager.acquire();
        if (!memory.valid()) {
            iterator->abort(std::make_error_code(std::errc::not_enough_memory));
            break;
        }

        auto response = std::make_shared<ScanResponse>(fiber, iterator, *socketIt.second, std::move(memory), scanId);
        iterator->addScanResponse(response);

        socketIt.second->scanStart(
            scanId,
            std::move(response),
            tableId,
            queryType,
            selectionLength,
            selection,
            queryLength,
            query,
            snapshot
        );
    }
    return iterator;
}

std::shared_ptr<TransferIterator> BaseClientProcessor::transferKeys(crossbow::infinio::Fiber& fiber, 
                                                                    commitmanager::Hash rangeStart,
                                                                    commitmanager::Hash rangeEnd,
                                                                    uint64_t tableId,
                                                                    const commitmanager::SnapshotDescriptor& snapshot, 
                                                                    Record record, 
                                                                    ScanMemoryManager& memoryManager,
                                                                    ScanQueryType queryType, 
                                                                    uint32_t selectionLength, 
                                                                    const char* selection, 
                                                                    uint32_t queryLength,
                                                                    const char* query) {
    auto scanId = ++mScanId;

    auto iterator = std::make_shared<TransferIterator>(fiber, std::move(record), mTellStoreSocket.size());
    for (auto& socketIt : mTellStoreSocket) {
        auto memory = memoryManager.acquire();
        if (!memory.valid()) {
            iterator->abort(std::make_error_code(std::errc::not_enough_memory));
            break;
        }

        auto response = std::make_shared<ScanResponse>(fiber, iterator, *socketIt.second, std::move(memory), scanId);
        iterator->addScanResponse(response);

        socketIt.second->transferKeys(
            rangeStart,
            rangeEnd,
            scanId,
            std::move(response),
            tableId,
            queryType,
            selectionLength,
            selection,
            queryLength,
            query,
            snapshot
        );
    }
    return iterator;
}

} // namespace store
} // namespace tell
