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

#include <tellstore/ClientConfig.hpp>
#include <tellstore/ClientManager.hpp>
#include <tellstore/GenericTuple.hpp>
#include <tellstore/Record.hpp>
#include <tellstore/ScanMemory.hpp>
#include <tellstore/TransactionRunner.hpp>

#include <crossbow/byte_buffer.hpp>
#include <crossbow/enum_underlying.hpp>
#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/logger.hpp>
#include <crossbow/program_options.hpp>
#include <crossbow/string.hpp>

#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <system_error>

using namespace tell;
using namespace tell::store;
using namespace tell::commitmanager;

namespace {

int64_t gTupleLargenumber = 0x7FFFFFFF00000001;
crossbow::string gTupleText1 = crossbow::string("Bacon ipsum dolor amet t-bone chicken prosciutto, cupim ribeye turkey "
        "bresaola leberkas bacon.");
crossbow::string gTupleText2 = crossbow::string("Chuck pork loin ham hock tri-tip pork ball tip drumstick tongue. Jowl "
        "swine short loin, leberkas andouille pancetta strip steak doner ham bresaola.");

class OperationTimer {
public:
    OperationTimer()
            : mTotalDuration(0x0u) {
    }

    void start() {
        mStartTime = std::chrono::steady_clock::now();
    }

    std::chrono::nanoseconds stop() {
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now()
                - mStartTime);
        mTotalDuration += duration;
        return duration;
    }

    std::chrono::nanoseconds total() const {
        return mTotalDuration;
    }

private:
    std::chrono::steady_clock::time_point mStartTime;
    std::chrono::nanoseconds mTotalDuration;
};

class TestClient {
public:
    TestClient(ClientConfig& config, size_t scanMemoryLength, size_t numTuple, size_t numTransactions);

    void run(bool check);

    void shutdown();

private:
    void addTable(ClientHandle& client);

    void executeTransaction(ClientHandle& client, uint64_t startKey, uint64_t endKey, bool check);

    void executeScan(ClientHandle& handle, float selectivity, bool check);

    void executeProjection(ClientHandle& client, float selectivity, bool check);

    void executeAggregation(ClientHandle& client, float selectivity);

    ClientManager<void> mManager;

    std::unique_ptr<ScanMemoryManager> mScanMemory;

    /// Number of tuples to insert per transaction
    size_t mNumTuple;

    /// Number of concurrent transactions to start
    size_t mNumTransactions;

    std::array<GenericTuple, 16> mTuple;

    Table mTable;
};

TestClient::TestClient(ClientConfig& config, size_t scanMemoryLength, size_t numTuple, size_t numTransactions)
        : mManager(config),
          mNumTuple(numTuple),
          mNumTransactions(numTransactions) {
        
    // Load initial routing information, so we can estimate scan memory parameters and create tables
    TransactionRunner::executeBlocking(mManager, [this, scanMemoryLength](ClientHandle& client) {
        auto clusterState = client.startTransaction();
        LOG_INFO("[TID %1%] Started transaction", clusterState->snapshot->version());
        client.commit(*clusterState->snapshot);

        this->mScanMemory = std::move(this->mManager.allocateScanMemory(
            clusterState->numPeers,
            scanMemoryLength / clusterState->numPeers
        ));
    });

    LOG_INFO("Initialized TellStore client");
    for (decltype(mTuple.size()) i = 0; i < mTuple.size(); ++i) {
        mTuple[i] = GenericTuple({
                std::make_pair<crossbow::string, boost::any>("number", static_cast<int32_t>(i)),
                std::make_pair<crossbow::string, boost::any>("largenumber", gTupleLargenumber),
                std::make_pair<crossbow::string, boost::any>("text1", gTupleText1),
                std::make_pair<crossbow::string, boost::any>("text2", gTupleText2)
        });
    }
}

void TestClient::run(bool check) {
    LOG_INFO("Starting test workload");
    auto startTime = std::chrono::steady_clock::now();

    LOG_INFO("Start create table transaction");
    TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::addTable, this, std::placeholders::_1));

    LOG_INFO("Starting %1% test load transaction(s)", mNumTransactions);
    MultiTransactionRunner<void> runner(mManager);
    for (decltype(mNumTransactions) i = 0; i < mNumTransactions; ++i) {
        auto startRange = i * mNumTuple;
        auto endRange = startRange + mNumTuple;
        auto tx = std::bind(&TestClient::executeTransaction, this, std::placeholders::_1, startRange, endRange, check);
        
        // runner.execute(tx);
        TransactionRunner::executeBlocking(mManager, tx);
    }
    runner.wait();

    LOG_INFO("Starting test scan transaction(s)");
    TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeScan, this, std::placeholders::_1, 1.0,
            check));
    TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeScan, this, std::placeholders::_1, 0.5,
            check));
    TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeScan, this, std::placeholders::_1, 0.25,
            check));
    TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeScan, this, std::placeholders::_1, 0.125,
            check));
    TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeScan, this, std::placeholders::_1,
            0.0625, check));
    TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeScan, this, std::placeholders::_1, 0,
            check));
    TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeProjection, this, std::placeholders::_1,
            1.0, check));
    TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeProjection, this, std::placeholders::_1,
            0.5, check));
    TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeProjection, this, std::placeholders::_1,
            0.25, check));
    TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeAggregation, this, std::placeholders::_1,
            1.0));
    TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeAggregation, this, std::placeholders::_1,
            0.5));
    TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeAggregation, this, std::placeholders::_1,
            0.25));

    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::duration<double>>(endTime - startTime);
    LOG_INFO("Running test workload took %1%s", duration.count());
}

void TestClient::shutdown() {
    LOG_INFO("Shutting down the TellStore client");

    mManager.shutdown();
}

void TestClient::addTable(ClientHandle& client) {
    LOG_TRACE("Adding table");
    Schema schema(TableType::TRANSACTIONAL);
    schema.addField(FieldType::INT, "number", true);
    schema.addField(FieldType::BIGINT, "largenumber", true);
    schema.addField(FieldType::TEXT, "text1", true);
    schema.addField(FieldType::TEXT, "text2", true);
    schema.addField(FieldType::HASH128, "__partition_token", false);

    auto startTime = std::chrono::steady_clock::now();
    mTable = client.createTable("testTable", schema);
    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
    LOG_INFO("Adding table took %1%ns", duration.count());

    client.createTable("testTable2", std::move(schema));
}

void TestClient::executeTransaction(ClientHandle& client, uint64_t startKey, uint64_t endKey, bool check) {
    LOG_TRACE("Starting transaction");
    auto clusterState = client.startTransaction();
    LOG_INFO("TID %1%] Started transaction with LAV: %2%", clusterState->snapshot->version(), clusterState->snapshot->lowestActiveVersion());

    OperationTimer insertTimer;
    OperationTimer getTimer;
    auto startTime = std::chrono::steady_clock::now();
    uint64_t errorCount = 0;
    for (auto key = startKey; key < endKey; ++key) {
        LOG_TRACE("Insert tuple");
        insertTimer.start();
        
        auto tupleData = mTuple[key % mTuple.size()];

        auto insertFuture = client.insert(mTable, key, *clusterState->snapshot, tupleData);
        if (auto ec = insertFuture->error()) {
            LOG_ERROR("Error inserting tuple [error = %1% %2%]", ec, ec.message());
            ++errorCount;
            break;
        }
        auto insertDuration = insertTimer.stop();
        LOG_DEBUG("Inserting tuple took %1%ns", insertDuration.count());

        LOG_TRACE("Get tuple");
        getTimer.start();
        auto getFuture = client.get(mTable, key, *clusterState->snapshot);
        if (!getFuture->waitForResult()) {
            auto& ec = getFuture->error();
            LOG_ERROR("Error getting tuple [error = %1% %2%]", ec, ec.message());
            ++errorCount;
            break;
        }
        auto getDuration = getTimer.stop();
        LOG_DEBUG("Getting tuple took %1%ns", getDuration.count());

        auto tuple = getFuture->get();
        if (tuple->version() != clusterState->snapshot->version()) {
            LOG_ERROR("Tuple not in the version written");
            ++errorCount;
            break;
        }
        if (!tuple->isNewest()) {
            LOG_ERROR("Tuple not the newest");
            ++errorCount;
            break;
        }

        if (check) {
            LOG_TRACE("Check tuple");

            auto numberValue = mTable.field<int32_t>("number", tuple->data());
            if (numberValue != static_cast<int32_t>(key % mTuple.size())) {
                LOG_ERROR("Number value of tuple %1% does not match [actual = %2%]", key, numberValue);
                ++errorCount;
                break;
            }

            auto largeNumberValue = mTable.field<int64_t>("largenumber", tuple->data());
            if (largeNumberValue != gTupleLargenumber) {
                LOG_ERROR("Largenumber value of tuple %1% does not match [actual = %2%]", key, largeNumberValue);
                ++errorCount;
                break;
            }

            auto text1Value = mTable.field<crossbow::string>("text1", tuple->data());
            if (text1Value != gTupleText1) {
                LOG_ERROR("Text1 value of tuple %1% does not match [actual = %2%]", key, text1Value);
                ++errorCount;
                break;
            }

            auto text2Value = mTable.field<crossbow::string>("text2", tuple->data());
            if (text2Value != gTupleText2) {
                LOG_ERROR("Text2 value of tuple %1% does not match [actual = %2%]", key, text2Value);
                ++errorCount;
                break;
            }

            LOG_TRACE("Tuple check successful");
        }
    }

    LOG_TRACE("Commit transaction");
    client.commit(*clusterState->snapshot);

    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    LOG_INFO("TID %1%] Transaction completed in %2%ms [total = %3%ms / %4%ms, average = %5%us / %6%us]",
             clusterState->snapshot->version(),
             duration.count(),
             std::chrono::duration_cast<std::chrono::milliseconds>(insertTimer.total()).count(),
             std::chrono::duration_cast<std::chrono::milliseconds>(getTimer.total()).count(),
             std::chrono::duration_cast<std::chrono::microseconds>(insertTimer.total()).count() / (endKey - startKey),
             std::chrono::duration_cast<std::chrono::microseconds>(getTimer.total()).count() / (endKey - startKey));
}

void TestClient::executeScan(ClientHandle& client, float selectivity, bool check) {
    LOG_TRACE("Starting transaction");
    auto& fiber = client.fiber();
    auto clusterState = client.startTransaction(TransactionType::READ_ONLY);
    LOG_INFO("TID %1%] Starting full scan with selectivity %2%%%", clusterState->snapshot->version(),
            static_cast<int>(selectivity * 100));

    Record::id_t recordField;
    if (!mTable.record().idOf("number", recordField)) {
        LOG_ERROR("number field not found");
        return;
    }

    uint32_t selectionLength = 32;
    std::unique_ptr<char[]> selection(new char[selectionLength]);

    crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
    selectionWriter.write<uint32_t>(0x1u); // Number of columns
    selectionWriter.write<uint16_t>(0x1u); // Number of conjuncts
    selectionWriter.write<uint16_t>(0x0u); // Partition shift
    selectionWriter.write<uint32_t>(0x0u); // Partition key
    selectionWriter.write<uint32_t>(0x0u); // Partition value
    selectionWriter.write<uint16_t>(recordField);
    selectionWriter.write<uint16_t>(0x1u);
    selectionWriter.align(sizeof(uint64_t));
    selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::GREATER_EQUAL));
    selectionWriter.write<uint8_t>(0x0u);
    selectionWriter.align(sizeof(uint32_t));
    selectionWriter.write<int32_t>(mTuple.size() - mTuple.size() * selectivity);

    auto scanStartTime = std::chrono::steady_clock::now();
    auto scanIterator = client.scan(mTable, *clusterState->snapshot, *mScanMemory, ScanQueryType::FULL, selectionLength,
            selection.get(), 0x0u, nullptr);

    size_t scanCount = 0x0u;
    size_t scanDataSize = 0x0u;
    while (scanIterator->hasNext()) {
        uint64_t key;
        const char* tuple;
        size_t tupleLength;
        std::tie(key, tuple, tupleLength) = scanIterator->next();
        ++scanCount;
        scanDataSize += tupleLength;

        if (check) {
            LOG_TRACE("Check tuple");

            auto numberValue = mTable.field<int32_t>("number", tuple);
            if (numberValue != static_cast<int32_t>(key % mTuple.size())) {
                LOG_ERROR("Number value of tuple %1% does not match [actual = %2%]", scanCount, numberValue);
                return;
            }

            auto largeNumberValue = mTable.field<int64_t>("largenumber", tuple);
            if (largeNumberValue != gTupleLargenumber) {
                LOG_ERROR("Largenumber value of tuple %1% does not match [actual = %2%]", scanCount, largeNumberValue);
                return;
            }

            auto text1Value = mTable.field<crossbow::string>("text1", tuple);
            if (text1Value != gTupleText1) {
                LOG_ERROR("Text1 value of tuple %1% does not match [actual = %2%]", scanCount, text1Value);
                return;
            }

            auto text2Value = mTable.field<crossbow::string>("text2", tuple);
            if (text2Value != gTupleText2) {
                LOG_ERROR("Text2 value of tuple %1% does not match [actual = %2%]", scanCount, text2Value);
                return;
            }

            LOG_TRACE("Tuple check successful");
            if (scanCount % 1000 == 0) {
                fiber.yield();
            }
        }
    }
    auto scanEndTime = std::chrono::steady_clock::now();

    if (scanIterator->error()) {
        auto& ec = scanIterator->error();
        LOG_ERROR("Error scanning table [error = %1% %2%]", ec, ec.message());
        return;
    }

    LOG_TRACE("Commit transaction");
    client.commit(*clusterState->snapshot);

    auto scanDuration = std::chrono::duration_cast<std::chrono::milliseconds>(scanEndTime - scanStartTime);
    auto scanTotalDataSize = double(scanDataSize) / double(1024 * 1024 * 1024);
    auto scanBandwidth = double(scanDataSize * 8) / double(1000 * 1000 * 1000 *
            std::chrono::duration_cast<std::chrono::duration<float>>(scanEndTime - scanStartTime).count());
    auto scanTupleSize = (scanCount == 0u ? 0u : scanDataSize / scanCount);
    LOG_INFO("TID %1%] Scan took %2%ms [%3% tuples of average size %4% (%5%GiB total, %6%Gbps bandwidth)]",
            clusterState->snapshot->version(), scanDuration.count(), scanCount, scanTupleSize, scanTotalDataSize, scanBandwidth);
}

void TestClient::executeProjection(ClientHandle& client, float selectivity, bool check) {
    LOG_TRACE("Starting transaction");
    auto& fiber = client.fiber();
    auto clusterState = client.startTransaction(TransactionType::READ_ONLY);
    LOG_INFO("TID %1%] Starting projection scan with selectivity %2%%%", clusterState->snapshot->version(),
            static_cast<int>(selectivity * 100));

    Record::id_t numberField;
    if (!mTable.record().idOf("number", numberField)) {
        LOG_ERROR("number field not found");
        return;
    }

    Record::id_t text2Field;
    if (!mTable.record().idOf("text2", text2Field)) {
        LOG_ERROR("text2 field not found");
        return;
    }

    uint32_t selectionLength = 32;
    std::unique_ptr<char[]> selection(new char[selectionLength]);

    crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
    selectionWriter.write<uint32_t>(0x1u); // Number of columns
    selectionWriter.write<uint16_t>(0x1u); // Number of conjuncts
    selectionWriter.write<uint16_t>(0x0u); // Partition shift
    selectionWriter.write<uint32_t>(0x0u); // Partition key
    selectionWriter.write<uint32_t>(0x0u); // Partition value
    selectionWriter.write<uint16_t>(numberField);
    selectionWriter.write<uint16_t>(0x1u);
    selectionWriter.align(sizeof(uint64_t));
    selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::GREATER_EQUAL));
    selectionWriter.write<uint8_t>(0x0u);
    selectionWriter.align(sizeof(uint32_t));
    selectionWriter.write<int32_t>(mTuple.size() - mTuple.size() * selectivity);

    uint32_t projectionLength = 4;
    std::unique_ptr<char[]> projection(new char[projectionLength]);

    crossbow::buffer_writer projectionWriter(projection.get(), projectionLength);
    projectionWriter.write<uint16_t>(numberField);
    projectionWriter.write<uint16_t>(text2Field);

    Schema resultSchema(mTable.tableType());
    resultSchema.addField(FieldType::INT, "number", true);
    resultSchema.addField(FieldType::TEXT, "text2", true);
    Table resultTable(mTable.tableId(), std::move(resultSchema));

    auto scanStartTime = std::chrono::steady_clock::now();
    auto scanIterator = client.scan(resultTable, *clusterState->snapshot, *mScanMemory, ScanQueryType::PROJECTION, selectionLength,
            selection.get(), projectionLength, projection.get());

    size_t scanCount = 0x0u;
    size_t scanDataSize = 0x0u;
    while (scanIterator->hasNext()) {
        uint64_t key;
        const char* tuple;
        size_t tupleLength;
        std::tie(key, tuple, tupleLength) = scanIterator->next();
        ++scanCount;
        scanDataSize += tupleLength;

        if (check) {
            LOG_TRACE("Check tuple");

            auto numberValue = resultTable.field<int32_t>("number", tuple);
            if (numberValue != static_cast<int32_t>(key % mTuple.size())) {
                LOG_ERROR("Number value of tuple %1% does not match [actual = %2%]", scanCount, numberValue);
                return;
            }

            auto text2Value = resultTable.field<crossbow::string>("text2", tuple);
            if (text2Value != gTupleText2) {
                LOG_ERROR("Text2 value of tuple %1% does not match [actual = %2%]", scanCount, text2Value);
                return;
            }

            LOG_TRACE("Tuple check successful");
            if (scanCount % 1000 == 0) {
                fiber.yield();
            }
        }
    }

    auto scanEndTime = std::chrono::steady_clock::now();
    if (scanIterator->error()) {
        auto& ec = scanIterator->error();
        LOG_ERROR("Error scanning table [error = %1% %2%]", ec, ec.message());
        return;
    }

    LOG_TRACE("Commit transaction");
    client.commit(*clusterState->snapshot);

    auto scanDuration = std::chrono::duration_cast<std::chrono::milliseconds>(scanEndTime - scanStartTime);
    auto scanTotalDataSize = double(scanDataSize) / double(1024 * 1024 * 1024);
    auto scanBandwidth = double(scanDataSize * 8) / double(1000 * 1000 * 1000 *
            std::chrono::duration_cast<std::chrono::duration<float>>(scanEndTime - scanStartTime).count());
    auto scanTupleSize = (scanCount == 0u ? 0u : scanDataSize / scanCount);
    LOG_INFO("TID %1%] Scan took %2%ms [%3% tuples of average size %4% (%5%GiB total, %6%Gbps bandwidth)]",
            clusterState->snapshot->version(), scanDuration.count(), scanCount, scanTupleSize, scanTotalDataSize, scanBandwidth);
}

void TestClient::executeAggregation(ClientHandle& client, float selectivity) {
    LOG_TRACE("Starting transaction");
    auto& fiber = client.fiber();
    auto clusterState = client.startTransaction(TransactionType::READ_ONLY);
    LOG_INFO("TID %1%] Starting aggregation scan with selectivity %2%%%", clusterState->snapshot->version(),
            static_cast<int>(selectivity * 100));

    Record::id_t recordField;
    if (!mTable.record().idOf("number", recordField)) {
        LOG_ERROR("number field not found");
        return;
    }

    uint32_t selectionLength = 32;
    std::unique_ptr<char[]> selection(new char[selectionLength]);

    crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
    selectionWriter.write<uint32_t>(0x1u); // Number of columns
    selectionWriter.write<uint16_t>(0x1u); // Number of conjuncts
    selectionWriter.write<uint16_t>(0x0u); // Partition shift
    selectionWriter.write<uint32_t>(0x0u); // Partition key
    selectionWriter.write<uint32_t>(0x0u); // Partition value
    selectionWriter.write<uint16_t>(recordField);
    selectionWriter.write<uint16_t>(0x1u);
    selectionWriter.align(sizeof(uint64_t));
    selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::GREATER_EQUAL));
    selectionWriter.write<uint8_t>(0x0u);
    selectionWriter.align(sizeof(uint32_t));
    selectionWriter.write<int32_t>(mTuple.size() - mTuple.size() * selectivity);

    uint32_t aggregationLength = 16;
    std::unique_ptr<char[]> aggregation(new char[aggregationLength]);

    crossbow::buffer_writer aggregationWriter(aggregation.get(), aggregationLength);
    aggregationWriter.write<uint16_t>(recordField);
    aggregationWriter.write<uint16_t>(crossbow::to_underlying(AggregationType::SUM));
    aggregationWriter.write<uint16_t>(recordField);
    aggregationWriter.write<uint16_t>(crossbow::to_underlying(AggregationType::MIN));
    aggregationWriter.write<uint16_t>(recordField);
    aggregationWriter.write<uint16_t>(crossbow::to_underlying(AggregationType::MAX));
    aggregationWriter.write<uint16_t>(recordField);
    aggregationWriter.write<uint16_t>(crossbow::to_underlying(AggregationType::CNT));

    Schema resultSchema(mTable.tableType());
    resultSchema.addField(FieldType::BIGINT, "sum", false);
    resultSchema.addField(FieldType::INT, "min", false);
    resultSchema.addField(FieldType::INT, "max", false);
    resultSchema.addField(FieldType::BIGINT, "cnt", true);
    Table resultTable(mTable.tableId(), std::move(resultSchema));

    auto scanStartTime = std::chrono::steady_clock::now();
    auto scanIterator = client.scan(resultTable, *clusterState->snapshot, *mScanMemory, ScanQueryType::AGGREGATION, selectionLength,
            selection.get(), aggregationLength, aggregation.get());

    size_t scanCount = 0x0u;
    size_t scanDataSize = 0x0u;
    int64_t totalSum = 0;
    int32_t totalMin = std::numeric_limits<int32_t>::max();
    int32_t totalMax = std::numeric_limits<int32_t>::min();
    int64_t totalCnt = 0;
    while (scanIterator->hasNext()) {
        const char* tuple;
        size_t tupleLength;
        std::tie(std::ignore, tuple, tupleLength) = scanIterator->next();
        ++scanCount;
        scanDataSize += tupleLength;

        totalSum += resultTable.field<int64_t>("sum", tuple);
        totalMin = std::min(totalMin, resultTable.field<int32_t>("min", tuple));
        totalMax = std::max(totalMax, resultTable.field<int32_t>("max", tuple));
        totalCnt += resultTable.field<int64_t>("cnt", tuple);

        if (scanCount % 1000 == 0) {
            fiber.yield();
        }
    }

    auto scanEndTime = std::chrono::steady_clock::now();
    if (scanIterator->error()) {
        auto& ec = scanIterator->error();
        LOG_ERROR("Error scanning table [error = %1% %2%]", ec, ec.message());
        return;
    }

    LOG_INFO("TID %1%] Scan output [sum = %2%, min = %3%, max = %4%, cnt = %5%]", clusterState->snapshot->version(), totalSum,
            totalMin, totalMax, totalCnt);

    LOG_TRACE("Commit transaction");
    client.commit(*clusterState->snapshot);

    auto scanDuration = std::chrono::duration_cast<std::chrono::milliseconds>(scanEndTime - scanStartTime);
    auto scanTotalDataSize = double(scanDataSize) / double(1024 * 1024 * 1024);
    auto scanBandwidth = double(scanDataSize * 8) / double(1000 * 1000 * 1000 *
            std::chrono::duration_cast<std::chrono::duration<float>>(scanEndTime - scanStartTime).count());
    auto scanTupleSize = (scanCount == 0u ? 0u : scanDataSize / scanCount);
    LOG_INFO("TID %1%] Scan took %2%ms [%3% tuples of average size %4% (%5%GiB total, %6%Gbps bandwidth)]",
            clusterState->snapshot->version(), scanDuration.count(), scanCount, scanTupleSize, scanTotalDataSize, scanBandwidth);
}

} // anonymous namespace

int main(int argc, const char** argv) {
    crossbow::string commitManagerHost;
    size_t scanMemoryLength = 0x80000000ull;
    size_t numTuple = 1000000ull;
    size_t numTransactions = 10;
    ClientConfig clientConfig;
    bool check = false;
    bool help = false;
    crossbow::string logLevel("DEBUG");

    auto opts = crossbow::program_options::create_options(argv[0],
            crossbow::program_options::value<'h'>("help", &help),
            crossbow::program_options::value<'l'>("log-level", &logLevel),
            crossbow::program_options::value<'c'>("commit-manager", &commitManagerHost),
            crossbow::program_options::value<'m'>("memory", &scanMemoryLength),
            crossbow::program_options::value<'n'>("tuple", &numTuple),
            crossbow::program_options::value<'t'>("transactions", &numTransactions),
            crossbow::program_options::value<-1>("network-threads", &clientConfig.numNetworkThreads,
                    crossbow::program_options::tag::ignore_short<true>{}),
            crossbow::program_options::value<-2>("check", &check,
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

    clientConfig.commitManager = ClientConfig::parseCommitManager(commitManagerHost);

    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);

    LOG_INFO("Starting TellStore test client%1%", check ? " (Check)" : "");
    LOG_INFO("--- Commit Manager: %1%", clientConfig.commitManager);
    LOG_INFO("--- Network Threads: %1%", clientConfig.numNetworkThreads);
    LOG_INFO("--- Scan Memory: %1%GB", double(scanMemoryLength) / double(1024 * 1024 * 1024));
    LOG_INFO("--- Number of tuples: %1%", numTuple);
    LOG_INFO("--- Number of transactions: %1%", numTransactions);

    // Initialize network stack
    TestClient client(clientConfig, scanMemoryLength, numTuple, numTransactions);
    client.run(check);
    client.shutdown();

    LOG_INFO("Exiting TellStore client");
    return 0;
}
