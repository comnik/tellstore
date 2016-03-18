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

#include <map>

namespace tell {
namespace store {
    
    /**
     * @brief Implementation of consistent hashing.
     */
    template <class Node>
    class HashRing {
        public:
            HashRing(unsigned int num_vnodes) : num_vnodes(num_vnodes), 
                                                hash_fn(std::hash<std::string>()) {}
            
            size_t insertNode(const std::string& nodeName, const Node& node);
            void removeNode(const std::string& nodeName);
            const Node* getNode(uint64_t key);

        private:
            const unsigned int num_vnodes;
            std::hash<std::string> hash_fn;
            std::map<size_t, Node> node_ring;
    };

    template <class Node>
    size_t HashRing<Node>::insertNode(const std::string& nodeName, const Node& node) {
        size_t hash;
        for (uint32_t vnode = 0; vnode < num_vnodes; vnode++) {
            hash = hash_fn(std::to_string(vnode) + nodeName);
            node_ring[hash] = node;
        }
        return hash;
    }

    template <class Node>
    void HashRing<Node>::removeNode(const std::string& nodeName) {
        size_t hash;
        for (uint32_t vnode = 0; vnode < num_vnodes; vnode++) {
            hash = hash_fn(std::to_string(vnode) + nodeName);
            node_ring.erase(hash);
        }
    }

    template <class Node>
    const Node* HashRing<Node>::getNode(uint64_t key) {
        if (node_ring.empty()) {
            return nullptr;
        } else {
            size_t hash = hash_fn(std::to_string(key));
            auto it = node_ring.lower_bound(hash);
            if (it == node_ring.end()) {
                it = node_ring.begin();
            }
            return &it->second;
        }
    }

} // namespace store
} // namespace tell
