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

#include "Table.hpp"

#include <config.h>
#include <util/PageManager.hpp>
#include <util/TableManager.hpp>
#include <util/VersionManager.hpp>

#include <crossbow/non_copyable.hpp>
#include <crossbow/string.hpp>

namespace tell {
namespace commitmanager {
class SnapshotDescriptor;
} // namespace commitmanager

namespace store {

class ScanQuery;

template<typename Context>
struct DeltaMainRewriteStore : crossbow::non_copyable, crossbow::non_movable {
    using Table = deltamain::Table<Context>;
    using GC = deltamain::GarbageCollector<Context>;

    static const char* implementationName() {
        return Context::implementationName();
    }

    DeltaMainRewriteStore(const StorageConfig& config)
        : mPageManager(PageManager::construct(config.totalMemory))
        , tableManager(*mPageManager, config, gc, mVersionManager)
    {
    }


    DeltaMainRewriteStore(const StorageConfig& config, size_t totalMem)
        : mPageManager(PageManager::construct(totalMem))
        , tableManager(*mPageManager, config, gc, mVersionManager)
    {
    }

    bool createTable(const uint64_t tableId,
                     const crossbow::string &name,
                     const Schema& schema) {
        return tableManager.createTable(tableId, name, schema, tableManager.config().hashMapCapacity);
    }

    bool createTable(const crossbow::string &name,
                     const Schema& schema,
                     uint64_t& idx)
    {
        return tableManager.createTable(name, schema, idx, tableManager.config().hashMapCapacity);
    }

    std::vector<const Table*> getTables() const
    {
        return tableManager.getTables();
    }

    const Table* getTable(uint64_t id) const
    {
        return tableManager.getTable(id);
    }

    const Table* getTable(const crossbow::string& name, uint64_t& id) const
    {
        return tableManager.getTable(name, id);
    }

    template <typename Fun>
    int get(uint64_t tableId, uint64_t key, const commitmanager::SnapshotDescriptor& snapshot, Fun fun)
    {
        return tableManager.get(tableId, key, snapshot, std::move(fun));
    }

    int update(uint64_t tableId, uint64_t key, size_t size, const char* data,
            const commitmanager::SnapshotDescriptor& snapshot)
    {
        return tableManager.update(tableId, key, size, data, snapshot);
    }

    int insert(uint64_t tableId, uint64_t key, size_t size, const char* data,
               const commitmanager::SnapshotDescriptor& snapshot)
    {
        return tableManager.insert(tableId, key, size, data, snapshot);
    }

    int remove(uint64_t tableId, uint64_t key, const commitmanager::SnapshotDescriptor& snapshot)
    {
        return tableManager.remove(tableId, key, snapshot);
    }

    int revert(uint64_t tableId, uint64_t key, const commitmanager::SnapshotDescriptor& snapshot)
    {
        return tableManager.revert(tableId, key, snapshot);
    }

    int scan(uint64_t tableId, ScanQuery* query)
    {
        return tableManager.scan(tableId, query);
    }

    /**
     * We use this method mostly for test purposes. But
     * it might be handy in the future as well. If possible,
     * this should be implemented in an efficient way.
     */
    void forceGC()
    {
        tableManager.forceGC();
    }

private:
    PageManager::Ptr mPageManager;
    GC gc;
    VersionManager mVersionManager;
    TableManager<Table, GC> tableManager;
};

using DeltaMainRewriteRowStore = DeltaMainRewriteStore<deltamain::RowStoreContext>;
using DeltaMainRewriteColumnStore = DeltaMainRewriteStore<deltamain::ColumnMapContext>;

} // namespace store
} // namespace tell
