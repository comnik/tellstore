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

#include <crossbow/logger.hpp>
#include <crossbow/string.hpp>

#include <commitmanager/HashRing.hpp>

#include <tellstore/ClientManager.hpp>

using namespace tell::commitmanager;
using namespace tell::store;


void dumpRanges(const HashRing& nodeRing) {
    LOG_INFO("== Ranges ====================");
    for (const auto& nodeIt : nodeRing.getRing()) {
        LOG_INFO("Node %1% ranges:", nodeIt.second.owner);
        for (const auto& range : nodeRing.getRanges(nodeIt.second.owner)) {
            LOG_INFO("\t[%1%, %2%]", HashRing::writeHash(range.start), HashRing::writeHash(range.end));
        }
    }
}

int main(int argc, const char** argv) {
    crossbow::string logLevel("INFO");
    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);

    HashRing nodeRing(1);

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

    return 0;
}