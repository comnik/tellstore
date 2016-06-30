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

#include <util/StorageConfig.hpp>

#include <commitmanager/ClientSocket.hpp>
#include <commitmanager/MessageTypes.hpp>
#include <commitmanager/HashRing.hpp>

#include <crossbow/allocator.hpp>
#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/logger.hpp>
#include <crossbow/program_options.hpp>
#include <crossbow/string.hpp>

#include <iostream>

using namespace tell::store;

// Scan queries have a fixed, known size
const uint32_t SELECTION_LENGTH = 128;

// Constructs a scan query that matches a specific key range
std::unique_ptr<char[]> createKeyTransferQuery(Table table, __int128 rangeStart, __int128 rangeEnd) {
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
    selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::GREATER));
    selectionWriter.write<uint8_t>(0x0u);           // predicate id
    selectionWriter.align(sizeof(__int128));
    selectionWriter.write<__int128>(rangeStart);     // second operand is rangeStart
    
    // And then key <= rangeEnd
    selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::LESS_EQUAL));
    selectionWriter.write<uint8_t>(0x1u);           // predicate id
    selectionWriter.align(sizeof(__int128));
    selectionWriter.write<__int128>(rangeEnd);       // second operand is rangeEnd

    return selection;
}

std::function<void (ClientHandle& client)> initializeRemote() {
    return [](ClientHandle& client) {
        auto snapshot = client.startTransaction(TransactionType::READ_WRITE);

        Schema schema(TableType::TRANSACTIONAL);
        schema.addField(FieldType::HASH128, "__partition_key", true);
        schema.addField(FieldType::INT, "number", true);
        schema.addField(FieldType::BIGINT, "largenumber", true);
        schema.addField(FieldType::TEXT, "text1", true);
        schema.addField(FieldType::TEXT, "text2", true);

        Table table1;
        Table table2;
        try {
            LOG_INFO("Creating remote tables...");
            table1 = client.createTable("testTable1", schema);
            table2 = client.createTable("testTable2", schema);
        } catch (const std::system_error& e) {
            LOG_INFO("Caught system_error with code %1% meaning %2%", e.code(), e.what());
            table1 = client.getTable("testTable1")->get();
            table2 = client.getTable("testTable2")->get();
        }

        int64_t gTupleLargenumber = 0x7FFFFFFF00000001;
        crossbow::string text1 = crossbow::string("Sometext");
        crossbow::string text2 = crossbow::string("Moretext");

        LOG_INFO("Populating remote tables...");
        for (uint64_t key = 1; key <= 100; ++key) {
            auto partitionKey = tell::commitmanager::HashRing<size_t>::getPartitionToken(table1.tableId(), key);
            auto testTuple = GenericTuple({
                std::make_pair<crossbow::string, boost::any>("__partition_key", partitionKey),
                std::make_pair<crossbow::string, boost::any>("number", static_cast<int32_t>(key)),
                std::make_pair<crossbow::string, boost::any>("largenumber", gTupleLargenumber),
                std::make_pair<crossbow::string, boost::any>("text1", text1),
                std::make_pair<crossbow::string, boost::any>("text2", text2)
            });

            if (key > 30) {
                // LOG_INFO("\t-> Inserting into table 2");
                auto insertFuture = client.insert(table2, key, *snapshot, testTuple);
                if (auto ec = insertFuture->error()) {
                    LOG_ERROR("\tError inserting tuple [error = %1% %2%]", ec, ec.message());
                }
            } else {
                // LOG_INFO("\t-> Inserting into table 1");
                auto insertFuture = client.insert(table1, key, *snapshot, testTuple);
                if (auto ec = insertFuture->error()) {
                    LOG_ERROR("\tError inserting tuple [error = %1% %2%]", ec, ec.message());
                }
            }
        }
        
        client.commit(*snapshot);
    };
}

// Initializes the local node from its peers
std::function<void (ClientHandle&)> schemaTransfer(Storage& storage) {
    return [&storage](ClientHandle& client) {
        LOG_INFO("Fetch remote tables...");
        auto tablesFuture = client.getTables();
        if (auto ec = tablesFuture->error()) {
            LOG_ERROR("Error fetching remote tables [error = %1% %2%]", ec, ec.message());
        }

        auto tables = tablesFuture->get();

        LOG_INFO("Creating %1% local tables...", tables.size());
        for (auto const& table : tables) {
            LOG_INFO("\t%1%", table.tableName());
            uint64_t tableId = table.tableId();
            auto succeeded = storage.createTable(table.tableName(), table.record().schema(), tableId);
            if (!succeeded) {
                LOG_ERROR("\tCould not create table %1%", table.tableName());
            }            
        }
    };
}

std::function<void (ClientHandle&)> keyTransfer(ClientManager<void>& manager, ClientConfig& config, Storage& storage, crossbow::string tableName, uint64_t rangeStart, uint64_t rangeEnd) {
    return [&manager, &config, &storage, &tableName, rangeStart, rangeEnd](ClientHandle& client) {
        // Allocate scan memory
        size_t scanMemoryLength = 0x80000000ull;
        std::unique_ptr<ScanMemoryManager> scanMemory = manager.allocateScanMemory(
            config.tellStore.size(),
            scanMemoryLength / config.tellStore.size()
        );

        try {
            auto snapshot = client.startTransaction(TransactionType::READ_ONLY);
        
            auto tableResponse = client.getTable(tableName);
            if (tableResponse->error()) {
                LOG_ERROR("Error fetching table %1%", tableName);
            }
            Table table = tableResponse->get();

            auto transferQuery = createKeyTransferQuery(table, rangeStart, rangeEnd); 

            LOG_INFO("Performing scan on table %1%", table.tableName());
            auto scanStartTime = std::chrono::steady_clock::now();    
            auto scanIterator = client.scan(
                table,
                *snapshot,
                *scanMemory,
                ScanQueryType::FULL,
                SELECTION_LENGTH,
                transferQuery.get(),
                0x0u,
                nullptr
            );

            size_t scanCount = 0x0u;
            size_t scanDataSize = 0x0u;
            while (scanIterator->hasNext()) {
                uint64_t key;
                const char* tuple;
                size_t tupleLength;
                std::tie(key, tuple, tupleLength) = scanIterator->next();
                ++scanCount;
                scanDataSize += tupleLength;

                auto tableId = table.tableId();

                auto ec = storage.insert(tableId, key, tupleLength, tuple, *snapshot);
                if (ec != 0) {
                    LOG_ERROR("Got error code %1%", ec);
                }
            }
            auto scanEndTime = std::chrono::steady_clock::now();

            if (scanIterator->error()) {
                auto& ec = scanIterator->error();
                LOG_ERROR("Error scanning table [error = %1% %2%]", ec, ec.message());
                return;
            }

            client.commit(*snapshot);

            auto scanDuration = std::chrono::duration_cast<std::chrono::milliseconds>(scanEndTime - scanStartTime);
            auto scanTotalDataSize = double(scanDataSize) / double(1024 * 1024 * 1024);
            auto scanBandwidth = double(scanDataSize * 8) / double(1000 * 1000 * 1000 *
                    std::chrono::duration_cast<std::chrono::duration<float>>(scanEndTime - scanStartTime).count());
            auto scanTupleSize = (scanCount == 0u ? 0u : scanDataSize / scanCount);
            LOG_INFO("TID %1%] Scan took %2%ms [%3% tuples of average size %4% (%5%GiB total, %6%Gbps bandwidth)]",
                    snapshot->version(), scanDuration.count(), scanCount, scanTupleSize, scanTotalDataSize, scanBandwidth);
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

    LOG_INFO("Initialize storage");
    Storage storage(storageConfig);

    LOG_INFO("Initialize network service");
    crossbow::infinio::InfinibandService service(infinibandLimits);

    ClientConfig clusterConfig;
    clusterConfig.commitManager = ClientConfig::parseCommitManager(directoryHost);

    ClientManager<void> clusterManager(clusterConfig);

    // Register with the commit-manager

    LOG_INFO("Registering with commit-manager...");

    std::unique_ptr<tell::commitmanager::ClusterMeta> clusterMeta;
    TransactionRunner::executeBlocking(clusterManager, [&ib0addr, &clusterMeta](ClientHandle& client) { 
        clusterMeta = std::move(client.registerNode(ib0addr, "STORAGE"));
    });

    // We registered successfully. This means, the commit manager should have 
    // responded with key ranges we are now responsible for. We have to
    // request these ranges from their current owners and then notify the commit manager that we now control them.

    // For this, we treat the set of current owners we need to contact, as a new cluster.

    for (auto range : clusterMeta->ranges) {
        if (range.owner == ib0addr) {
            LOG_INFO("\t-> first owner of range [%1%, %2%]", (uint64_t) range.start, (uint64_t) range.end);
        } else {
            LOG_INFO("\t-> request range [%1%, %2%] from %3%", (uint64_t) range.start, (uint64_t) range.end, range.owner);
            clusterConfig.tellStore.emplace_back(crossbow::infinio::Endpoint::ipv4(), range.owner);
        }
    }

    if (clusterConfig.tellStore.size() > 0) {
        // Re-initialize the ClientManager
        
        ClientManager<void> clusterManager(clusterConfig);
        
        // Initialize and populate the remote nodes

        auto initializeTx = initializeRemote();
        TransactionRunner::executeBlocking(clusterManager, initializeTx);

        // Perform the schema transfer

        auto schemaTransferTx = schemaTransfer(storage);
        TransactionRunner::executeBlocking(clusterManager, schemaTransferTx);

        // Perform the key transfer across all tables
        
        for (auto range : clusterMeta->ranges) {
            for (auto const& table : storage.getTables()) {
                auto tx = keyTransfer(clusterManager, clusterConfig, storage, table->tableName(), range.start, range.end);
                TransactionRunner::executeBlocking(clusterManager, tx);
            }
        }

        // Ensure the tuples are available locally now

        LOG_INFO("Verifying key transfer...");
        auto snapshot = ClientHandle::createAnalyticalSnapshot(0, std::numeric_limits<uint64_t>::max());
        char* data = new char[256];
        auto tupleLocation = [&data](size_t size, uint64_t version, bool isNewest) { 
            return data;
        };

        uint64_t tupleCount = 0;
        for (uint64_t key = 1; key <= 50; ++key) {
            for (auto const& table : storage.getTables()) {
                auto ec = storage.get(table->tableId(), key, *snapshot, tupleLocation);
                if (ec == 0) {
                    tupleCount++;
                } else {
                    // LOG_ERROR("\t-> failed to retrieve tuple %1% from table %2% (ec %3%)", key, table->tableName(), ec);
                }
            }
        }
        LOG_ASSERT(tupleCount == 49, "Could not retrieve all tuples!");
        LOG_INFO("Found %1% tuples", tupleCount);
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
