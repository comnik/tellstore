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
#include "ServerConfig.hpp"
#include "ServerSocket.hpp"
#include "Storage.hpp"

#include <tellstore/ClientConfig.hpp>
#include <tellstore/ClientManager.hpp>
#include <tellstore/TransactionRunner.hpp>
#include <tellstore/GenericTuple.hpp>
#include <tellstore/ErrorCode.hpp>

#include <util/StorageConfig.hpp>

#include <commitmanager/ClientSocket.hpp>
#include <commitmanager/MessageTypes.hpp>
#include <commitmanager/HashRing.hpp>

#include <crossbow/allocator.hpp>
#include <crossbow/infinio/Endpoint.hpp>
#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/logger.hpp>
#include <crossbow/program_options.hpp>
#include <crossbow/string.hpp>

#include <iostream>
#include <set>
#include <vector>
#include <functional>
#include <cmath>
#include <limits>

using namespace tell::store;
using namespace tell::commitmanager;
using namespace std::placeholders;

/// Scan queries have a fixed, known size
const uint32_t SELECTION_LENGTH = 136;

using HashRing_t = tell::commitmanager::HashRing<crossbow::string>;


// Constructs a scan query that matches a specific key range
std::unique_ptr<char[]> createKeyTransferQuery(Table table, Hash rangeStart, Hash rangeEnd) {
    std::unique_ptr<char[]> selection(new char[SELECTION_LENGTH]);

    crossbow::buffer_writer selectionWriter(selection.get(), SELECTION_LENGTH);
    selectionWriter.write<uint32_t>(0x1u);      // Number of columns
    selectionWriter.write<uint16_t>(0x2u);      // Number of conjuncts
    selectionWriter.write<uint16_t>(0x0u);      // Partition shift
    selectionWriter.write<uint32_t>(0x0u);      // Partition key
    selectionWriter.write<uint32_t>(0x0u);      // Partition value
    
    // Selection on the partition key
    Record::id_t keyField;
    table.record().idOf("__partition_key", keyField);
    LOG_ASSERT(keyField >= 0, "Schema must have an partition key defined.");

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

// Initializes the local node from its peers
std::function<void (ClientHandle&)> schemaTransfer(Storage& storage) {
    return [&storage](ClientHandle& client) {
        LOG_INFO("Fetching remote tables...");
        auto tablesFuture = client.getTables();
        if (auto ec = tablesFuture->error()) {
            LOG_ERROR("Error fetching remote tables [error = %1% %2%]", ec, ec.message());
        }

        auto tables = tablesFuture->get();

        LOG_INFO("Creating %1% local tables...", tables.size());
        for (const auto& table : tables) {
            LOG_INFO("\t%1%", table.tableName());
            uint64_t tableId = table.tableId();
            auto succeeded = storage.createTable(table.tableName(), table.record().schema(), tableId);
            if (!succeeded) {
                LOG_ERROR("\tCould not create table %1%", table.tableName());
            }            
        }
    };
}

std::function<void (ClientHandle&)> keyTransfer(ClientManager<void>& manager, std::shared_ptr<ClientConfig> config, Storage& storage, crossbow::string tableName, Hash rangeStart, Hash rangeEnd) {
    return [&manager, &config, &storage, &tableName, rangeStart, rangeEnd](ClientHandle& client) {
        // Allocate scan memory
        size_t scanMemoryLength = 0x80000000ull;
        std::unique_ptr<ScanMemoryManager> scanMemory = manager.allocateScanMemory(
            config->numStores(),
            scanMemoryLength / config->numStores()
        );

        try {
            auto clusterState = client.startTransaction(TransactionType::READ_ONLY);
        
            auto table = client.getTable(tableName)->get();

            auto transferQuery = createKeyTransferQuery(table, rangeStart, rangeEnd); 

            LOG_INFO("Requesting key transfer on table '%1%' @ version %2% @ LAV: %3%", table.tableName(), clusterState->snapshot->version(), clusterState->snapshot->lowestActiveVersion());
            auto scanIterator = client.transferKeys(
                rangeStart,
                rangeEnd,
                table,
                *clusterState->snapshot,
                *scanMemory,
                ScanQueryType::FULL,
                SELECTION_LENGTH,
                transferQuery.get(),
                0x0u,
                nullptr
            );

            client.commit(*clusterState->snapshot);

            size_t scanCount = 0x0u;
            while (scanIterator->hasNext()) {
                uint64_t key;
                const char* tuple;
                size_t tupleLength;
                std::tie(key, tuple, tupleLength) = scanIterator->next();
                ++scanCount;

                LOG_INFO("\treceived Tuple %1%", key);

                auto tableId = table.tableId();
                auto ec = storage.insert(tableId, key, tupleLength, tuple, *clusterState->snapshot);
                if (ec != 0) {
                    LOG_ERROR("\tInsertion failed with error code %1%", ec);
                }
            }

            if (scanIterator->error()) {
                auto& ec = scanIterator->error();
                LOG_ERROR("Error scanning table [error = %1% %2%]", ec, ec.message());
                return;
            }

            LOG_INFO("[TID %1%] Received %2% tuples in total", clusterState->snapshot->version(), scanCount);
        } catch (const std::system_error& e) {
            LOG_INFO("Caught system_error with code %1% meaning %2%", e.code(), e.what());
        }
    };
}

int main(int argc, const char** argv) {
    StorageConfig storageConfig;
    ServerConfig serverConfig;

    // ib0 address
    crossbow::string ib0addr;
    // Host to register at
    crossbow::string directoryHost;
    
    bool help = false;
    crossbow::string logLevel("DEBUG");

    auto opts = crossbow::program_options::create_options(argv[0],
            crossbow::program_options::value<'h'>("help", &help),
            crossbow::program_options::value<'l'>("log-level", &logLevel),
            crossbow::program_options::value<'d'>("directory", &directoryHost),
            crossbow::program_options::value<'a'>("host", &ib0addr),
            crossbow::program_options::value<'p'>("port", &serverConfig.port),
            crossbow::program_options::value<'m'>("memory", &storageConfig.totalMemory),
            crossbow::program_options::value<'c'>("capacity", &storageConfig.hashMapCapacity),
            crossbow::program_options::value<-1>("network-threads", &serverConfig.numNetworkThreads,
                    crossbow::program_options::tag::ignore_short<true>{}),
            crossbow::program_options::value<-2>("scan-threads", &storageConfig.numScanThreads,
                    crossbow::program_options::tag::ignore_short<true>{}),
            crossbow::program_options::value<-3>("gc-interval", &storageConfig.gcInterval,
                    crossbow::program_options::tag::ignore_short<true>{}));

    try {
        crossbow::program_options::parse(opts, argc, argv);
    } catch (crossbow::program_options::argument_not_found e) {
        std::cerr << e.what() << std::endl << std::endl;
        crossbow::program_options::print_help(std::cout, opts);
        return 1;
    }

    if (help) {
        crossbow::program_options::print_help(std::cout, opts);
        return 0;
    }

    crossbow::infinio::InfinibandLimits infinibandLimits;
    infinibandLimits.receiveBufferCount = 1024;
    infinibandLimits.sendBufferCount = 256;
    infinibandLimits.bufferLength = 128 * 1024;
    infinibandLimits.sendQueueLength = 128;
    infinibandLimits.completionQueueLength = 2048;

    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);

    LOG_INFO("Starting TellStore server");
    LOG_INFO("--- Backend: %1%", Storage::implementationName());
    LOG_INFO("--- Directory: %1%", directoryHost);
    LOG_INFO("--- Host: %1%", ib0addr);
    LOG_INFO("--- Port: %1%", serverConfig.port);
    LOG_INFO("--- Network Threads: %1%", serverConfig.numNetworkThreads);
    LOG_INFO("--- GC Interval: %1%s", storageConfig.gcInterval);
    LOG_INFO("--- Total Memory: %1%GB", double(storageConfig.totalMemory) / double(1024 * 1024 * 1024));
    LOG_INFO("--- Scan Threads: %1%", storageConfig.numScanThreads);
    LOG_INFO("--- Hash Map Capacity: %1%", storageConfig.hashMapCapacity);

    // Initialize allocator
    crossbow::allocator::init();

    LOG_INFO("Initializing storage");
    Storage storage(storageConfig);

    LOG_INFO("Initializing network service");
    crossbow::infinio::InfinibandService service(infinibandLimits);

    auto clusterConfig = std::make_shared<ClientConfig>();
    clusterConfig->commitManager = ClientConfig::parseCommitManager(directoryHost);

    ClientManager<void> clusterManager(clusterConfig);

    // Register with the commit-manager

    LOG_INFO("Registering with commit-manager...");

    std::unique_ptr<ClusterMeta> clusterMeta;
    TransactionRunner::executeBlocking(clusterManager, [&ib0addr, &clusterMeta](ClientHandle& client) { 
        clusterMeta = std::move(client.registerNode(ib0addr, "STORAGE"));
    });

    // We registered successfully. This means, the commit manager should have 
    // responded with key ranges we are now responsible for. We have to
    // request these ranges from their current owners and then notify the commit manager that we now control them.

    // For this, we treat the set of current owners we need to contact, as a new cluster.

    std::set<crossbow::string> owners;
    for (auto range : clusterMeta->ranges) {
        if (range.owner == ib0addr) {
            LOG_INFO("\t-> first owner of range [%1%, %2%]", HashRing_t::writeHash(range.start), HashRing_t::writeHash(range.end));
        } else {
            LOG_INFO("\t-> request range [%1%, %2%] from %3%", HashRing_t::writeHash(range.start), HashRing_t::writeHash(range.end), range.owner);
            owners.insert(range.owner);
        }
    }

    std::vector<crossbow::infinio::Endpoint> ownerEndpoints;
    ownerEndpoints.reserve(owners.size());

    for (const auto& owner : owners) {
        ownerEndpoints.emplace_back(crossbow::infinio::Endpoint::ipv4(), owner);
    }

    clusterConfig->setStores(ownerEndpoints);

    if (clusterConfig->numStores() > 0) {
        // Re-initialize the ClientManager
        
        clusterManager.reloadConfig(clusterConfig);
        
        // Perform the schema transfer

        auto schemaTransferTx = schemaTransfer(storage);
        TransactionRunner::executeBlocking(clusterManager, schemaTransferTx);

        // Perform the key transfer across all tables
        
        for (auto range : clusterMeta->ranges) {
            for (const auto& table : storage.getTables()) {
                auto tx = keyTransfer(clusterManager, clusterConfig, storage, table->tableName(), range.start, range.end);
                TransactionRunner::executeBlocking(clusterManager, tx);
            }
        }
    }
    
    LOG_INFO("Initialize network server");
    ServerManager server(service, storage, serverConfig);
    service.run();

    LOG_INFO("Exiting TellStore server");

    TransactionRunner::executeBlocking(clusterManager, [&ib0addr](ClientHandle& client) {
        LOG_INFO("Unregistering with commit-manager...");
        client.unregisterNode(ib0addr);
    });
    
    return 0;
}
