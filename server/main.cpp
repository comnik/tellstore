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

// Struct describing a partition [start, end] and the responsible node
struct Partition {
    const crossbow::string owner;
    uint64_t start;
    uint64_t end;

    Partition(crossbow::string owner, uint64_t start, uint64_t end) :
                owner(owner),
                start(start),
                end(end)
                {}
};

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

        std::unique_ptr<std::vector<Partition>> ranges(new std::vector<Partition>());
        ranges->emplace_back("localhost:7243", 1, 50);

        // We registered successfully. This means, the commit manager should have 
        // responded with key ranges we are now responsible for. We have to
        // request these ranges from their current owners and then notify the commit manager that we now control them.

        // For this, we treat the set of current owners we need to contact, as a new cluster.
        ClientConfig ownersConfig;
        for (auto range : *ranges) {
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
        LOG_INFO("We have to fetch some keys");
        ownersConfig.commitManager = ClientConfig::parseCommitManager(directoryHost);
        ClientManager<void> ownersManager(ownersConfig);

        LOG_INFO("About to start the scan");

        // Now we perform a scan for all the ranges.
        TransactionRunner::executeBlocking(ownersManager, [](ClientHandle& client) {
            LOG_INFO("Initiating scan...");
        
            auto& fiber = client.fiber();
            auto snapshot = client.startTransaction(TransactionType::READ_ONLY);

            // Populate with some dummy data

            LOG_INFO("Adding table");
            Schema schema(TableType::TRANSACTIONAL);
            schema.addField(FieldType::INT, "number", true);
            schema.addField(FieldType::BIGINT, "largenumber", true);
            schema.addField(FieldType::TEXT, "text1", true);
            schema.addField(FieldType::TEXT, "text2", true);

            Table mTable = client.createTable("testTable", schema);
            LOG_INFO("Added 'testTable'");

            int64_t gTupleLargenumber = 0x7FFFFFFF00000001;
            size_t i = 0;
            crossbow::string text1 = crossbow::string("Sometext");
            crossbow::string text2 = crossbow::string("Moretext");
            auto tuple = GenericTuple({
                std::make_pair<crossbow::string, boost::any>("number", static_cast<int32_t>(i)),
                std::make_pair<crossbow::string, boost::any>("largenumber", gTupleLargenumber),
                std::make_pair<crossbow::string, boost::any>("text1", text1),
                std::make_pair<crossbow::string, boost::any>("text2", text2)
            });
            
            auto insertFuture = client.insert(mTable, 1, *snapshot, tuple);
            if (auto ec = insertFuture->error()) {
                LOG_ERROR("Error inserting tuple [error = %1% %2%]", ec, ec.message());
                return;
            } else {
                LOG_INFO("Inserted tuple.");
            }

            // Record::id_t recordField;
            // if (!mTable.record().idOf("number", recordField)) {
            //     LOG_ERROR("number field not found");
            //     return;
            // }

            // uint32_t selectionLength = 32;
            // std::unique_ptr<char[]> selection(new char[selectionLength]);

            // crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
            // selectionWriter.write<uint32_t>(0x1u); // Number of columns
            // selectionWriter.write<uint16_t>(0x1u); // Number of conjuncts
            // selectionWriter.write<uint16_t>(0x0u); // Partition shift
            // selectionWriter.write<uint32_t>(0x0u); // Partition key
            // selectionWriter.write<uint32_t>(0x0u); // Partition value
            // selectionWriter.write<uint16_t>(recordField);
            // selectionWriter.write<uint16_t>(0x1u);
            // selectionWriter.align(sizeof(uint64_t));
            // selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::GREATER_EQUAL));
            // selectionWriter.write<uint8_t>(0x0u);
            // selectionWriter.align(sizeof(uint32_t));
            // selectionWriter.write<int32_t>(mTuple.size() - mTuple.size() * selectivity);
  
            // client.scan(mTable, *snapshot, *mScanMemory, ScanQueryType::FULL, selectionLength, selection.get(), 0x0u, nullptr);
        });
    }

    LOG_INFO("Initialize network server");
    ServerManager server(service, storage, serverConfig);
    service.run();

    LOG_INFO("Exiting TellStore server");
    return 0;
}
