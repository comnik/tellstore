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
 *     Nikolas Göbel <ngoebel@student.ethz.ch>
 */

#include <crossbow/logger.hpp>
#include <crossbow/string.hpp>
#include <crossbow/program_options.hpp>

#include <commitmanager/HashRing.hpp>

#include <tellstore/ClientManager.hpp>

#include <iostream>
#include <fstream>
#include <chrono>

using namespace tell::commitmanager;
using namespace tell::store;


/**
 * Prints out the current set of storage nodes and
 * the partitions they are responsible for.
 */
void dumpRanges (const HashRing& nodeRing) {
    LOG_INFO("== Ranges ====================");
    for (const auto& nodeIt : nodeRing.getRing()) {
        LOG_INFO("Node %1% ranges:", nodeIt.second.owner);
        for (const auto& range : nodeRing.getRanges(nodeIt.second.owner)) {
            LOG_INFO("[%1%, %2%]", HashRing::writeHash(range.start), HashRing::writeHash(range.end));
        }
    }
}


/**
 * Tests wether partitions are correctly
 * adapted whenever nodes join or leave.
 */
void testPartitioning (uint32_t numVnodes) {
    LOG_INFO("::Partitioning Test");

    HashRing nodeRing(numVnodes);

    Hash node1Token = nodeRing.insertNode("0.0.0.0:7243");
    dumpRanges(nodeRing);

    Hash node2Token = nodeRing.insertNode("0.0.0.0:7244");
    dumpRanges(nodeRing);

    Hash node1Key = node1Token -1 ; // a key guaranteed to lie inside node1's partition
    auto partition1 = nodeRing.getNode(node1Key);
    LOG_INFO("-- key 1 owner: %1%", partition1->owner);
    LOG_INFO("-- previous owner: %1%", partition1->previousOwner);
    
    Hash node2Key = node2Token - 1; // a key guaranteed to lie inside node2's partition
    auto partition2 = nodeRing.getNode(node2Key);
    LOG_INFO("-- key 2 owner: %1%", partition2->owner);
    LOG_INFO("-- previous owner: %1%", partition2->previousOwner);
    LOG_INFO("\n");

    auto ranges = nodeRing.removeNode("0.0.0.0:7243");
    for (const auto& range : ranges) {
        LOG_INFO("\t[%1%, %2%] -> %3%", HashRing::writeHash(range.start), HashRing::writeHash(range.end), range.owner);
    }
}


void testDistribution (uint64_t numKeys, uint32_t numNodes, uint32_t numVnodes) {
    LOG_INFO("::Distribution Test");

    HashRing ring(numVnodes);

    std::unordered_map<crossbow::string, uint32_t> nodeIds;
    nodeIds.reserve(numNodes);

    for (uint32_t i=1; i <= numNodes; ++i) {
        crossbow::string host = "node" + crossbow::to_string(i) + ".example.com";
        
        ring.insertNode(host);
        nodeIds[host] = i;
    }

    auto moduloShard  = [numNodes] (uint64_t key) { return (key % numNodes) + 1; }; 
    auto md5Shard     = [&ring, &nodeIds] (uint64_t key) { return nodeIds[ring.getNode(1, key)->owner]; };
    auto murmur3Shard = [&ring, &nodeIds] (uint64_t key) { return nodeIds[ring.getNode(1, key)->owner]; };

    std::ofstream moduloResults("results/modulo-" + std::to_string(numVnodes) + "vnodes.csv");
    std::ofstream md5Results("results/md5-" + std::to_string(numVnodes) + "vnodes.csv");
    std::ofstream murmur3Results("results/murmur3-" + std::to_string(numVnodes) + "vnodes.csv");

    for (uint64_t i=0; i < numKeys; ++i) {
        moduloResults   << moduloShard(i)   << std::endl;
        md5Results      << md5Shard(i)      << std::endl;
        murmur3Results  << murmur3Shard(i)  << std::endl;
    }

    moduloResults.close();
    md5Results.close();
    murmur3Results.close();
}


std::chrono::milliseconds baselinePerformance (uint64_t numKeys, uint32_t numNodes) {
    auto startTime = std::chrono::steady_clock::now();

    for (uint64_t i=0; i < numKeys; ++i) {
        auto partition = i % numNodes;
    }

    auto endTime = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
}


std::chrono::milliseconds testPerformance (uint64_t numKeys, uint32_t numVnodes) {
    HashRing ring(numVnodes);

    auto startTime = std::chrono::steady_clock::now();

    for (uint64_t i=0; i < numKeys; ++i) {
        auto partition = ring.getNode(1, i);
    }

    auto endTime = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
}


int main (int argc, const char** argv) {
    bool help = false;
    crossbow::string logLevel("INFO");
    uint64_t numKeys = 1000000;
    uint32_t numNodes = 10;
    uint32_t numVnodes = 1;

    auto opts = crossbow::program_options::create_options(argv[0],
            crossbow::program_options::value<'h'>("help", &help),
            crossbow::program_options::value<'l'>("log-level", &logLevel),
            crossbow::program_options::value<'k'>("keys", &numKeys),
            crossbow::program_options::value<'n'>("nodes", &numNodes),
            crossbow::program_options::value<'v'>("vnodes", &numVnodes));

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

    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);

    LOG_INFO("Starting Consistent Hashing Test");
    LOG_INFO("--- Number of keys: %1%", numKeys);
    LOG_INFO("--- Number of nodes: %1%", numNodes);
    LOG_INFO("--- Number of virtual nodes: %1%", numVnodes);

    // testPartitioning(numVnodes);
    // testDistribution(numKeys, numNodes, numVnodes);

    auto baselineDuration = baselinePerformance(numKeys, numNodes);
    LOG_INFO("Baseline duration: %1%ms", baselineDuration.count());

    std::ofstream perfResults("perf/murmur3.csv");
    for (uint32_t vnodes = 1; vnodes <= 200; vnodes += 10) {
        auto duration = testPerformance(numKeys, vnodes);
        // LOG_INFO("Partitioning %1% keys with %2% took %3%ms", numKeys, numVnodes, duration.count());
        perfResults << vnodes << "," << duration.count() << std::endl;
    }
    perfResults.close();
    
    return 0;
}