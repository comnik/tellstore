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

#include "llvm/ADT/STLExtras.h"
#include "llvm/Analysis/Passes.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Vectorize.h"
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/Bitcode/BitcodeWriterPass.h"
#include <cctype>
#include <cstdio>
#include <map>
#include <string>
#include <vector>
#include "LLVMJIT.hpp"

using namespace llvm;
using namespace llvm::orc;

/**
 * @brief The LLVMCodeGenerator class is used as a utility class to all LLVM-related
 * operations in TELL.
 *
 * The typical usage is to first create an LLVMContext (use the getGlobalContext if
 * you are single-threaded) and an LLVMJIT (using getJIT). Next, you can create as
 * many LLVM modules as you like using the created context and jit (using the get
 * methods of this classe).
 * For code creation
 * in a module you might want to use an IRBuilder which you create with the same
 * context as the module.
 *
 * Code in a module can be executed in the following way using its jit:
 * - auto H = jit->addModule(std::move(module));
 * - auto exprSymbol = jit->findSymbol(...);
 * - ret-type (*funcPtr) (args) = (ret-type (*)(args))(intptr_t) exprSymbol.getAddress();
 * - use funcPtr to execute the function(s) as often as you like
 * - jit->removeModule(H);
 *
 * You can optimize certain functions in a module by using a FunctionPassManager which
 * you can get with the constructor listed in this class. Then simply call
 * run(function) on the function you want to optimize.
 *
 * If you use the functions
 */

class LLVMCodeGenerator {

public:

static std::unique_ptr<LLVMJIT> getJIT() {
    if (!mInitialized)
        initialize();
    return std::move(llvm::make_unique<LLVMJIT>());
}

static std::unique_ptr<Module> getModule(LLVMJIT *jit, LLVMContext &context, std::string name) {
    std::unique_ptr<Module> module = llvm::make_unique<Module>(name, context);
    module->setDataLayout(jit->getTargetMachine().createDataLayout());
    return std::move(module);
}

static std::unique_ptr<legacy::FunctionPassManager> getFunctionPassManger(Module *module) {
    // create a function pass manager attached to the module with some optimizations
    std::unique_ptr<legacy::FunctionPassManager> functionPassMgr = llvm::make_unique<legacy::FunctionPassManager>(module);
    // Provide basic AliasAnalysis support for GVN.
    functionPassMgr->add(createBasicAliasAnalysisPass());
    // Do simple "peephole" optimizations and bit-twiddling optzns.
    functionPassMgr->add(createInstructionCombiningPass());
    // Reassociate expressions.
    functionPassMgr->add(createReassociatePass());
    // Eliminate Common SubExpressions.
    functionPassMgr->add(createGVNPass());
    // Simplify the control flow graph (deleting unreachable blocks, etc).
    functionPassMgr->add(createCFGSimplificationPass());
    // add basic block vectorization
    functionPassMgr->add(createBBVectorizePass());
    // add loop vectorization
    functionPassMgr->add(createLoopVectorizePass());
    // initialize function pass manager
    functionPassMgr->doInitialization();
    return std::move(functionPassMgr);
}

private:

static void initialize() {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    InitializeNativeTargetAsmParser();
    mInitialized = true;
}

static bool mInitialized;

};

bool LLVMCodeGenerator::mInitialized = false;