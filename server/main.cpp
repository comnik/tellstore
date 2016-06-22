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

#include <crossbow/allocator.hpp>
#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/logger.hpp>
#include <crossbow/program_options.hpp>
#include <crossbow/string.hpp>

#include <iostream>

using namespace tell::store;


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

    LOG_INFO("Register with cluster directory");
    std::unique_ptr<crossbow::infinio::InfinibandProcessor> processor = service.createProcessor();
    tell::commitmanager::ClientSocket commitManagerSocket(service.createSocket(*processor));

    crossbow::infinio::Endpoint endpoint(crossbow::infinio::Endpoint::ipv4(), directoryHost);
    commitManagerSocket.connect(endpoint);

    processor->executeFiber([&ib0addr, &commitManagerSocket, &directoryHost] (crossbow::infinio::Fiber& fiber) {
        auto registerResponse = commitManagerSocket.registerNode(fiber, ib0addr, "STORAGE");
        if (!registerResponse->waitForResult()) {
            auto& ec = registerResponse->error();
            LOG_INFO("Error while registering [error = %1% %2%]", ec, ec.message());
            return;
        }

        auto clusterMeta = registerResponse->get();
        
        // We registered successfully. This means, the commit manager should have 
        // responded with key ranges we are now responsible for. We have to
        // request these ranges from their current owners and then notify the commit manager that we now control them.

        // For this, we treat the set of current owners we need to contact, as a new cluster.
        ClientConfig ownersConfig;
        for (auto range : clusterMeta->ranges) {
            if (range.owner == ib0addr) {
                LOG_INFO("This node is the first owner of range [%1%, %2%]", range.start, range.end);
            } else {
                LOG_INFO("Will request range [%1%, %2%] from %3%...", range.start, range.end, range.owner);
                ownersConfig.tellStore.emplace_back(crossbow::infinio::Endpoint::ipv4(), range.owner);
            }
        }
    });

    ClientConfig ownersConfig;
    if ("localhost:7243" != ib0addr) {
        ownersConfig.tellStore.emplace_back(crossbow::infinio::Endpoint::ipv4(), "localhost:7243");
    }

    if (ownersConfig.tellStore.size() > 0) {
        ownersConfig.commitManager = ClientConfig::parseCommitManager(directoryHost);
        ClientManager<void> ownersManager(ownersConfig);

        size_t scanMemoryLength = 0x80000000ull;
        std::unique_ptr<ScanMemoryManager> scanMemory(ownersManager.allocateScanMemory(ownersConfig.tellStore.size(), scanMemoryLength / ownersConfig.tellStore.size()));
        
        // Now we perform a scan for all the ranges.
        TransactionRunner::executeBlocking(ownersManager, [&scanMemory, &storage](ClientHandle& client) {
            LOG_INFO("Initiating scan...");
        
            auto& fiber = client.fiber();
            auto snapshot = client.startTransaction(TransactionType::READ_ONLY);
            
            // The schema has to be globally available.
            // All nodes (old and new) must be initialized with the same schema.

            Schema schema(TableType::TRANSACTIONAL);
            schema.addField(FieldType::INT, "number", true);
            schema.addField(FieldType::BIGINT, "largenumber", true);
            schema.addField(FieldType::TEXT, "text1", true);
            schema.addField(FieldType::TEXT, "text2", true);

            crossbow::string tableName("testTable");

            // Initialize remote node and populate with dummy data

            LOG_INFO("Creating remote table...");
            Table table = client.createTable(tableName, schema);

            int64_t gTupleLargenumber = 0x7FFFFFFF00000001;
            size_t i = 10;
            crossbow::string text1 = crossbow::string("Sometext");
            crossbow::string text2 = crossbow::string("Moretext");
            auto testTuple = GenericTuple({
                std::make_pair<crossbow::string, boost::any>("number", static_cast<int32_t>(i)),
                std::make_pair<crossbow::string, boost::any>("largenumber", gTupleLargenumber),
                std::make_pair<crossbow::string, boost::any>("text1", text1),
                std::make_pair<crossbow::string, boost::any>("text2", text2)
            });
            
            LOG_INFO("Populating remote table...");
            auto insertFuture = client.insert(table, 1, *snapshot, testTuple);
            if (auto ec = insertFuture->error()) {
                LOG_ERROR("Error inserting tuple [error = %1% %2%]", ec, ec.message());
            }

            // Initialize local node from previous owners

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
            
            // Finally perform the actual key transfer

            uint32_t selectionLength = 56;
            std::unique_ptr<char[]> selection(new char[selectionLength]);

            crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
            selectionWriter.write<uint32_t>(0x1u);      // Number of columns
            selectionWriter.write<uint16_t>(0x1u);      // Number of conjuncts
            selectionWriter.write<uint16_t>(0x0u);      // Partition shift
            selectionWriter.write<uint32_t>(0x0u);      // Partition key
            selectionWriter.write<uint32_t>(0x0u);      // Partition value
            
            // Selection on the internal tell key
            Record::id_t keyField = -1;
            selectionWriter.write<uint16_t>(keyField); // -1 is the fixed id of the tell key field
            selectionWriter.write<uint16_t>(0x1u);      // number of predicates
            selectionWriter.align(sizeof(uint64_t));        
            
            // We need a predicate P(key) := range_start <= key < range_end
            // Start with range_start <= key
            selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::GREATER));
            selectionWriter.write<uint8_t>(0x0u);       // predicate id
            selectionWriter.align(sizeof(uint64_t));
            selectionWriter.write<int64_t>(0);       // second operand is range_start
            // And then key < range_end
            // selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::LESS_EQUAL));
            // selectionWriter.write<uint8_t>(0x1u);       // predicate id
            // selectionWriter.align(sizeof(uint32_t));
            // selectionWriter.write<int64_t>(50);      // second operand is range_end

            auto scanStartTime = std::chrono::steady_clock::now();    
            auto scanIterator = client.scan(table, *snapshot, *scanMemory, ScanQueryType::FULL, selectionLength, selection.get(), 0x0u, nullptr);

            size_t scanCount = 0x0u;
            size_t scanDataSize = 0x0u;
            while (scanIterator->hasNext()) {
                uint64_t key;
                const char* tuple;
                size_t tupleLength;
                std::tie(key, tuple, tupleLength) = scanIterator->next();
                ++scanCount;
                scanDataSize += tupleLength;

                auto text1Value = table.field<crossbow::string>("text1", tuple);
                LOG_INFO("Got tuple with text %1%", text1Value);

                auto tableId = table.tableId();

                auto ec = storage.insert(tableId, key, tupleLength, tuple, *snapshot);
                LOG_INFO("Got error code %1%", ec);
            }
            auto scanEndTime = std::chrono::steady_clock::now();

            if (scanIterator->error()) {
                auto& ec = scanIterator->error();
                LOG_ERROR("Error scanning table [error = %1% %2%]", ec, ec.message());
                return;
            }

            LOG_INFO("Committing transaction");
            client.commit(*snapshot);

            auto scanDuration = std::chrono::duration_cast<std::chrono::milliseconds>(scanEndTime - scanStartTime);
            auto scanTotalDataSize = double(scanDataSize) / double(1024 * 1024 * 1024);
            auto scanBandwidth = double(scanDataSize * 8) / double(1000 * 1000 * 1000 *
                    std::chrono::duration_cast<std::chrono::duration<float>>(scanEndTime - scanStartTime).count());
            auto scanTupleSize = (scanCount == 0u ? 0u : scanDataSize / scanCount);
            LOG_INFO("TID %1%] Scan took %2%ms [%3% tuples of average size %4% (%5%GiB total, %6%Gbps bandwidth)]",
                    snapshot->version(), scanDuration.count(), scanCount, scanTupleSize, scanTotalDataSize, scanBandwidth);

            // Ensure the tuples are available locally now

            LOG_INFO("Verifying key transfer...");
            char* data = new char[256];
            auto ec = storage.get(table.tableId(), 1, *snapshot, [&data](size_t size, uint64_t version, bool isNewest) { 
                return data; 
            });
            auto text1Value = table.field<crossbow::string>("text1", data);
            LOG_INFO("Got tuple with text %1%", text1Value);

            LOG_INFO("Got error code %1%", ec);
        });
    }

    LOG_INFO("Initialize network server");
    ServerManager server(service, storage, serverConfig);
    service.run();

    LOG_INFO("Exiting TellStore server");
    return 0;
}
