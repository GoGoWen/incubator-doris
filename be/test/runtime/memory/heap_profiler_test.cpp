// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/memory/heap_profiler.h"

#include <gtest/gtest.h>

#include <string>

namespace doris {

class HeapProfilerTest : public testing::Test {
protected:
    void SetUp() override { profiler = std::make_unique<HeapProfiler>(); }

    std::unique_ptr<HeapProfiler> profiler;
};

TEST_F(HeapProfilerTest, StartAndStop) {
    profiler->heap_profiler_start();
    profiler->heap_profiler_stop();
}

TEST_F(HeapProfilerTest, CheckHeapProfiler) {
    bool result = profiler->check_heap_profiler();
#ifndef USE_JEMALLOC
    EXPECT_FALSE(result);
#endif
}

#if defined(USE_JEMALLOC) && !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && \
        !defined(THREAD_SANITIZER)

TEST_F(HeapProfilerTest, ProfilerStateTransitions) {
    // Initially should be inactive
    EXPECT_FALSE(profiler->check_heap_profiler());

    // Start profiler
    profiler->heap_profiler_start();
    EXPECT_TRUE(profiler->check_heap_profiler());

    // Stop profiler
    profiler->heap_profiler_stop();
    EXPECT_FALSE(profiler->check_heap_profiler());
}

TEST_F(HeapProfilerTest, MultipleStartCalls) {
    // Multiple starts should be safe
    profiler->heap_profiler_start();
    EXPECT_TRUE(profiler->check_heap_profiler());

    profiler->heap_profiler_start();
    EXPECT_TRUE(profiler->check_heap_profiler());

    profiler->heap_profiler_stop();
}

TEST_F(HeapProfilerTest, MultipleStopCalls) {
    // Multiple stops should be safe
    profiler->heap_profiler_start();
    profiler->heap_profiler_stop();
    EXPECT_FALSE(profiler->check_heap_profiler());

    profiler->heap_profiler_stop();
    EXPECT_FALSE(profiler->check_heap_profiler());
}

TEST_F(HeapProfilerTest, DumpProfileWithActiveProfiler) {
    // Start profiler
    profiler->heap_profiler_start();
    EXPECT_TRUE(profiler->check_heap_profiler());

    // Allocate some memory to track
    std::vector<char*> allocations;
    for (int i = 0; i < 10; ++i) {
        allocations.push_back(new char[1024]);
    }

    // Dump profile
    std::string result = profiler->dump_heap_profile();

    // Clean up
    for (char* ptr : allocations) {
        delete[] ptr;
    }

    profiler->heap_profiler_stop();

    // Should return a valid file path
    if (!result.empty()) {
        EXPECT_NE(result.find("jeheap_dump"), std::string::npos);
        EXPECT_NE(result.find(".heap"), std::string::npos);
    }
}

#endif // USE_JEMALLOC && !SANITIZER

TEST_F(HeapProfilerTest, DumpHeapProfile) {
    std::string result = profiler->dump_heap_profile();
#ifdef USE_JEMALLOC
    // With jemalloc, should return a file path containing "jeheap_dump" and ".heap"
    if (!result.empty()) {
        EXPECT_NE(result.find("jeheap_dump"), std::string::npos);
        EXPECT_NE(result.find(".heap"), std::string::npos);
    }
#else
    EXPECT_TRUE(result.empty());
#endif
}

TEST_F(HeapProfilerTest, DumpHeapProfileToDot) {
    std::string result = profiler->dump_heap_profile_to_dot();
#ifndef USE_JEMALLOC
    EXPECT_TRUE(result.empty());
#endif
    // With jemalloc, result depends on jeprof execution success
    // Just verify the function executes without crashing
}

TEST_F(HeapProfilerTest, CreateGlobalInstance) {
    HeapProfiler* instance = HeapProfiler::create_global_instance();
    EXPECT_NE(instance, nullptr);
    delete instance;
}

} // namespace doris
