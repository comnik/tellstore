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

#include "LLVMScan.hpp"

#include "LLVMRowAggregation.hpp"
#include "LLVMRowProjection.hpp"
#include "LLVMRowScan.hpp"

#include <tellstore/Record.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/alignment.hpp>
#include <crossbow/byte_buffer.hpp>
#include <crossbow/enum_underlying.hpp>
#include <crossbow/logger.hpp>

#include <llvm/ADT/ArrayRef.h>
#include <llvm/Analysis/Passes.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Vectorize.h>

#include <array>
#include <sstream>
#include <string>

namespace tell {
namespace store {

namespace {

const std::string ROW_MATERIALIZE_NAME = "rowMaterialize.";

uint32_t memcpyWrapper(const char* src, uint32_t length, char* dest) {
    memcpy(dest, src, length);
    return length;
}

} // anonymous namespace

LLVMCodeModule::LLVMCodeModule(const std::string& name)
        : mModule(name, mContext) {
    mModule.setDataLayout(mCompiler.getTargetMachine()->createDataLayout());
    mModule.setTargetTriple(mCompiler.getTargetMachine()->getTargetTriple().getTriple());
}

LLVMCodeModule::~LLVMCodeModule() {
    mCompiler.removeModule(mHandle);
}

void LLVMCodeModule::compile() {
#ifndef NDEBUG
    LOG_INFO("[Module = %1%] Dumping LLVM scan code", mModule.getModuleIdentifier());
    mModule.dump();

    LOG_INFO("[Module = %1%] Verifying LLVM scan code", mModule.getModuleIdentifier());
    if (llvm::verifyModule(mModule, &llvm::dbgs())) {
        LOG_FATAL("[Module = %1%] Verifying LLVM scan code failed", mModule.getModuleIdentifier());
        std::terminate();
    }
#endif

    // Compile the module
    mHandle = mCompiler.addModule(&mModule);
}

LLVMScanBase::LLVMScanBase(const Record& record, std::vector<ScanQuery*> queries)
        : mRecord(record),
          mQueries(std::move(queries)),
          mQueryModule("ScanQuery"),
          mMaterializationModule("Materialization") {
    using namespace llvm;

    LLVMBuilder builder(mQueryModule.getModule().getContext());

    mScanAst.numConjunct = mQueries.size();
    mScanAst.conjunctProperties.resize(mQueries.size(), {0});

    std::unordered_map<QueryDataHolder, size_t> queryCache;
    for (decltype(mQueries.size()) i = 0; i < mQueries.size(); ++i) {
        LOG_DEBUG("Accessing next query");
        auto q = mQueries[i];

        crossbow::buffer_reader queryReader(q->selection(), q->selectionLength());
        auto snapshot = q->snapshot();

        auto numColumns = queryReader.read<uint32_t>();

        QueryAST queryAst;
        queryAst.baseVersion = snapshot->baseVersion();
        queryAst.version = snapshot->version();
        queryAst.shared = false;
        queryAst.conjunctOffset = mScanAst.numConjunct;
        queryAst.numConjunct = queryReader.read<uint16_t>();
        queryAst.partitionShift = queryReader.read<uint16_t>();
        queryAst.partitionModulo = queryReader.read<uint32_t>();
        queryAst.partitionNumber = queryReader.read<uint32_t>();

        if (queryAst.partitionModulo != 0) {
            mScanAst.needsKey = true;
        }

        if (queryAst.numConjunct != 0) {
            QueryDataHolder holder(queryReader.data(), queryReader.end());
            auto j = queryCache.find(holder);
            if (j != queryCache.end()) {
                auto& sharedQuery = mScanAst.queries[j->second];
                sharedQuery.shared = true;

                queryAst.shared = true;
                queryAst.conjunctOffset = sharedQuery.conjunctOffset;
                mScanAst.queries.emplace_back(std::move(queryAst));
                continue;
            }
            queryCache.emplace(holder, i);
            mScanAst.conjunctProperties.insert(mScanAst.conjunctProperties.end(), queryAst.numConjunct, {i});
        }

        for (decltype(numColumns) j = 0; j < numColumns; ++j) {
            auto currentColumn = queryReader.read<uint16_t>();
            auto numPredicates = queryReader.read<uint16_t>();
            queryReader.advance(4);

            // Add a new FieldAST if the field does not yet exist
            auto iter = mScanAst.fields.find(currentColumn);
            if (iter == mScanAst.fields.end()) {
                FieldAST fieldAst;

                if (currentColumn == INTERNAL_KEY_FIELD_ID) {
                    // Client wants to perform a scan on the internal tell key

                    LOG_DEBUG("Scan on internal tell key detected");

                    fieldAst.id          = INTERNAL_KEY_FIELD_ID;
                    fieldAst.isNotNull   = true;
                    fieldAst.nullIdx     = 0;
                    fieldAst.isFixedSize = true;
                    fieldAst.isInternal  = true;
                    fieldAst.type        = FieldType::BIGINT;
                    fieldAst.size        = sizeof(int64_t);

                } else {
                    LOG_DEBUG("Scan on regular field detected");

                    auto& fieldMeta = mRecord.getFieldMeta(currentColumn);
                    auto& field = fieldMeta.field;

                    fieldAst.id          = currentColumn;
                    fieldAst.isNotNull   = field.isNotNull();
                    fieldAst.nullIdx     = (field.isNotNull() ? 0 : fieldMeta.nullIdx);
                    fieldAst.isFixedSize = field.isFixedSized();
                    fieldAst.type        = field.type();
                    fieldAst.offset      = fieldMeta.offset;
                    fieldAst.alignment   = field.alignOf();
                    fieldAst.size        = field.staticSize();

                    mScanAst.needsNull |= !field.isNotNull();
                }

                LOG_DEBUG("Inserting new field node...");

                auto res = mScanAst.fields.emplace(currentColumn, std::move(fieldAst));
                LOG_ASSERT(res.second, "Field already in map");
                iter = res.first;
            }
            auto& fieldAst = iter->second;

            // Iterate over all predicates on the field
            for (decltype(numPredicates) j = 0; j < numPredicates; ++j) {
                LOG_DEBUG("Parsing predicate %1%", j);

                auto predicateType = queryReader.read<PredicateType>();
                auto conjunct = queryAst.conjunctOffset + queryReader.read<uint8_t>();
                ++mScanAst.conjunctProperties[conjunct].predicateCount;

                PredicateAST predicateAst(predicateType, conjunct);

                if (predicateType == PredicateType::IS_NULL || predicateType == PredicateType::IS_NOT_NULL) {
                    queryReader.advance(6);
                } else {
                    fieldAst.needsValue = true;

                    switch (fieldAst.type) {
                    case FieldType::SMALLINT: {
                        predicateAst.fixed.value = builder.getInt16(queryReader.read<int16_t>());
                        predicateAst.fixed.predicate = builder.getIntPredicate(predicateType);
                        predicateAst.fixed.isFloat = false;
                        queryReader.advance(4);
                    } break;

                    case FieldType::INT: {
                        queryReader.advance(2);
                        predicateAst.fixed.value = builder.getInt32(queryReader.read<int32_t>());
                        predicateAst.fixed.predicate = builder.getIntPredicate(predicateType);
                        predicateAst.fixed.isFloat = false;
                    } break;

                    case FieldType::BIGINT: {
                        LOG_DEBUG("Getting int64_t value for predicate");
                        queryReader.advance(6);
                        predicateAst.fixed.value = builder.getInt64(queryReader.read<int64_t>());
                        predicateAst.fixed.predicate = builder.getIntPredicate(predicateType);
                        predicateAst.fixed.isFloat = false;
                    } break;

                    case FieldType::FLOAT: {
                        queryReader.advance(2);
                        predicateAst.fixed.value = builder.getFloat(queryReader.read<float>());
                        predicateAst.fixed.predicate = builder.getFloatPredicate(predicateType);
                        predicateAst.fixed.isFloat = true;
                    } break;

                    case FieldType::DOUBLE: {
                        queryReader.advance(6);
                        predicateAst.fixed.value = builder.getDouble(queryReader.read<double>());
                        predicateAst.fixed.predicate = builder.getFloatPredicate(predicateType);
                        predicateAst.fixed.isFloat = true;
                    } break;

                    case FieldType::BLOB:
                    case FieldType::TEXT: {
                        queryReader.advance(2);
                        auto size = queryReader.read<uint32_t>();
                        auto data = queryReader.read(size);
                        queryReader.align(8u);

                        predicateAst.variable.size = size;
                        predicateAst.variable.prefix = 0;
                        if (size > 0) {
                            memcpy(&predicateAst.variable.prefix, data,
                                    size < sizeof(uint32_t) ? size : sizeof(uint32_t));

                            auto value = ConstantDataArray::get(builder.getContext(),
                                    makeArrayRef(reinterpret_cast<const uint8_t*>(data), size));
                            predicateAst.variable.value = new GlobalVariable(mQueryModule.getModule(), value->getType(),
                                    true, GlobalValue::PrivateLinkage, value);
                        }
                    } break;

                    default: {
                        LOG_ASSERT(false, "Invalid field");
                    } break;
                    }
                }
                LOG_DEBUG("Inserting new predicate node");
                fieldAst.predicates.emplace_back(std::move(predicateAst));
            }
        }

        mScanAst.numConjunct += queryAst.numConjunct;
        LOG_DEBUG("Inserting new query node");
        mScanAst.queries.emplace_back(std::move(queryAst));
    }
    LOG_DEBUG("Done.");
    LOG_ASSERT(mScanAst.queries.size() == mQueries.size(), "Did not process every query");
    LOG_ASSERT(mScanAst.conjunctProperties.size() == mScanAst.numConjunct, "Number of conjuncts does not match");
}

LLVMRowScanBase::LLVMRowScanBase(const Record& record, std::vector<ScanQuery*> queries)
        : LLVMScanBase(record, std::move(queries)),
          mRowScanFun(nullptr) {
}

void LLVMRowScanBase::prepareQuery() {
    LOG_ASSERT(!mRowScanFun, "Scan already finalized");
    LLVMRowScanBuilder::createFunction(mQueryModule.getModule(), mQueryModule.getTargetMachine(), mScanAst);

    mQueryModule.compile();

    mRowScanFun = mQueryModule.findFunction<RowScanFun>(LLVMRowScanBuilder::FUNCTION_NAME);
}

void LLVMRowScanBase::prepareMaterialization() {
    LOG_ASSERT(mRowMaterializeFuns.empty(), "Scan already finalized");
    std::unordered_map<QueryDataHolder, std::string> materializeCache;
    for (decltype(mQueries.size()) i = 0; i < mQueries.size(); ++i) {
        auto q = mQueries[i];
        if (q->queryType() == ScanQueryType::FULL) {
            continue;
        }

        QueryDataHolder holder(q->query(), q->queryLength(), crossbow::to_underlying(q->queryType()));
        if (materializeCache.find(holder) != materializeCache.end()) {
            continue;
        }

        std::stringstream ss;
        ss << ROW_MATERIALIZE_NAME << i;
        auto name = ss.str();
        materializeCache.emplace(holder, name);

        switch (q->queryType()) {
        case ScanQueryType::PROJECTION: {
            LLVMRowProjectionBuilder::createFunction(mRecord, mMaterializationModule.getModule(),
                    mMaterializationModule.getTargetMachine(), name, q);
        } break;

        case ScanQueryType::AGGREGATION: {
            LLVMRowAggregationBuilder::createFunction(mRecord, mMaterializationModule.getModule(),
                    mMaterializationModule.getTargetMachine(), name, q);
        } break;

        default: {
            LOG_ASSERT(false, "Unknown query type");
        } break;
        }
    }

    mMaterializationModule.compile();

    for (decltype(mQueries.size()) i = 0; i < mQueries.size(); ++i) {
        auto q = mQueries[i];

        if (q->queryType() == ScanQueryType::FULL) {
            mRowMaterializeFuns.emplace_back(&memcpyWrapper);
            continue;
        }

        QueryDataHolder holder(q->query(), q->queryLength(), crossbow::to_underlying(q->queryType()));
        auto& name = materializeCache.at(holder);
        auto fun = mMaterializationModule.findFunction<RowMaterializeFun>(name);
        mRowMaterializeFuns.emplace_back(fun);
    }
}

LLVMRowScanProcessorBase::LLVMRowScanProcessorBase(const Record& record, const std::vector<ScanQuery*>& queries,
        LLVMRowScanBase::RowScanFun rowScanFunc,
        const std::vector<LLVMRowScanBase::RowMaterializeFun>& rowMaterializeFuns, uint32_t numConjuncts)
        : mRecord(record),
          mRowScanFun(rowScanFunc),
          mRowMaterializeFuns(rowMaterializeFuns),
          mNumConjuncts(numConjuncts),
          mResult(mNumConjuncts, 0u) {
    LOG_ASSERT(mNumConjuncts >= queries.size(), "More queries than conjuncts");

    mQueries.reserve(queries.size());
    for (auto q : queries) {
        mQueries.emplace_back(q->createProcessor());
    }
}

void LLVMRowScanProcessorBase::processRowRecord(uint64_t key, uint64_t validFrom, uint64_t validTo, const char* data,
        uint32_t length) {
    LOG_ASSERT(mResult.size() >= mNumConjuncts, "Result array must be larger or equal than number of conjuncts");

    LOG_INFO("Performing scan on key %1%", key);
    mRowScanFun(key, validFrom, validTo, data, &mResult.front());

    for (decltype(mQueries.size()) i = 0; i < mQueries.size(); ++i) {
        // Check if the selection string matches the record
        if (mResult[i] == 0) {
            continue;
        }

        auto fun = mRowMaterializeFuns[i];
        mQueries[i].writeRecord(key, length, validFrom, validTo, [fun, data, length] (char* dest) {
            return fun(data, length, dest);
        });
    }
}

} // namespace store
} // namespace tell
