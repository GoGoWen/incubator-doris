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

#include "http/action/jeprofile_actions.h"

#include <gtest/gtest.h>

#include "http/http_request.h"
#include "runtime/exec_env.h"
#include "runtime/memory/heap_profiler.h"

namespace doris {

class JeprofileActionsTest : public testing::Test {
public:
    JeprofileActionsTest() { _exec_env = ExecEnv::GetInstance(); }

    virtual ~JeprofileActionsTest() = default;

    void SetUp() override {
        HeapProfiler::instance()->heap_profiler_stop();
    }

    void TearDown() override {
        HeapProfiler::instance()->heap_profiler_stop();
    }

protected:
    ExecEnv* _exec_env = nullptr;
};

#if defined(USE_JEMALLOC) && !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && \
        !defined(THREAD_SANITIZER)

// Tests for SetJeHeapProfileActiveActions with jemalloc enabled
TEST_F(JeprofileActionsTest, SetJeHeapProfileActive_StartProfiler) {
    auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest request(evhttp_req);
    (*request.params())["prof_value"] = "true";

    EXPECT_FALSE(HeapProfiler::instance()->check_heap_profiler());

    SetJeHeapProfileActiveActions action(_exec_env);
    action.handle(&request);

    EXPECT_TRUE(HeapProfiler::instance()->check_heap_profiler());
}

TEST_F(JeprofileActionsTest, SetJeHeapProfileActive_StopProfiler) {
    // Start profiler first
    HeapProfiler::instance()->heap_profiler_start();
    EXPECT_TRUE(HeapProfiler::instance()->check_heap_profiler());

    auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest request(evhttp_req);
    (*request.params())["prof_value"] = "false";

    SetJeHeapProfileActiveActions action(_exec_env);
    action.handle(&request);

    EXPECT_FALSE(HeapProfiler::instance()->check_heap_profiler());
}

TEST_F(JeprofileActionsTest, SetJeHeapProfileActive_InvalidParameter) {
    auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest request(evhttp_req);
    (*request.params())["prof_value"] = "invalid";

    SetJeHeapProfileActiveActions action(_exec_env);
    action.handle(&request);

    // Invalid value should be treated as false (stop profiler)
    EXPECT_FALSE(HeapProfiler::instance()->check_heap_profiler());
}

TEST_F(JeprofileActionsTest, SetJeHeapProfileActive_EmptyParameter) {
    auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest request(evhttp_req);
    (*request.params())["prof_value"] = "";

    SetJeHeapProfileActiveActions action(_exec_env);
    action.handle(&request);

    // Empty value should be treated as false (stop profiler)
    EXPECT_FALSE(HeapProfiler::instance()->check_heap_profiler());
}

TEST_F(JeprofileActionsTest, DumpJeHeapProfileToDot_ExecutesWithoutCrash) {
    // Start profiler
    HeapProfiler::instance()->heap_profiler_start();

    auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest request(evhttp_req);
    DumpJeHeapProfileToDotActions action(_exec_env);

    // Should execute without crashing
    ASSERT_NO_THROW(action.handle(&request));
}

TEST_F(JeprofileActionsTest, DumpJeHeapProfile_ExecutesWithoutCrash) {
    // Start profiler
    HeapProfiler::instance()->heap_profiler_start();

    auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest request(evhttp_req);
    DumpJeHeapProfileActions action(_exec_env);

    // Should execute without crashing
    ASSERT_NO_THROW(action.handle(&request));
}

TEST_F(JeprofileActionsTest, WorkflowIntegration_StartDumpStop) {
    // Step 1: Start profiler
    {
        auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest request(evhttp_req);
        (*request.params())["prof_value"] = "true";
        SetJeHeapProfileActiveActions action(_exec_env);
        action.handle(&request);
        EXPECT_TRUE(HeapProfiler::instance()->check_heap_profiler());
    }

    // Step 2: Dump heap profile (should not crash)
    {
        evhttp_request* req2 = evhttp_request_new(nullptr, nullptr);
        HttpRequest request(req2);
        DumpJeHeapProfileActions action(_exec_env);
        ASSERT_NO_THROW(action.handle(&request));
    }

    // Step 3: Dump to dot (should not crash)
    {
        evhttp_request* req3 = evhttp_request_new(nullptr, nullptr);
        HttpRequest request(req3);
        DumpJeHeapProfileToDotActions action(_exec_env);
        ASSERT_NO_THROW(action.handle(&request));
    }

    // Step 4: Stop profiler
    {
        evhttp_request* req4 = evhttp_request_new(nullptr, nullptr);
        HttpRequest request(req4);
        (*request.params())["prof_value"] = "false";
        SetJeHeapProfileActiveActions action(_exec_env);
        action.handle(&request);
        EXPECT_FALSE(HeapProfiler::instance()->check_heap_profiler());
    }
}

TEST_F(JeprofileActionsTest, MultipleStartStopCalls) {
    for (int i = 0; i < 3; ++i) {
        {
            auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
            HttpRequest start_req(evhttp_req);
            (*start_req.params())["prof_value"] = "true";
            SetJeHeapProfileActiveActions start_action(_exec_env);
            start_action.handle(&start_req);
            EXPECT_TRUE(HeapProfiler::instance()->check_heap_profiler());

        }

        {
            auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
            HttpRequest stop_req(evhttp_req);
            (*stop_req.params())["prof_value"] = "false";
            SetJeHeapProfileActiveActions stop_action(_exec_env);
            stop_action.handle(&stop_req);
            EXPECT_FALSE(HeapProfiler::instance()->check_heap_profiler());

        }
    }
}

#else

TEST_F(JeprofileActionsTest, SetJeHeapProfileActive_NonJemallocBuild) {
    auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest request(evhttp_req);
    (*request.params())["prof_value"] = "true";

    SetJeHeapProfileActiveActions action(_exec_env);

    ASSERT_NO_THROW(action.handle(&request));

    EXPECT_FALSE(HeapProfiler::instance()->check_heap_profiler());
}

TEST_F(JeprofileActionsTest, DumpJeHeapProfileToDot_NonJemallocBuild) {
    auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest request(evhttp_req);
    DumpJeHeapProfileToDotActions action(_exec_env);

    ASSERT_NO_THROW(action.handle(&request));
}

TEST_F(JeprofileActionsTest, DumpJeHeapProfile_NonJemallocBuild) {
    auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest request(evhttp_req);
    DumpJeHeapProfileActions action(_exec_env);

    ASSERT_NO_THROW(action.handle(&request));
}

#endif

}
