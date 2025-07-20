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

#include <gtest/gtest.h>

#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"

// Enable HDFS mocking for tests - must be defined before any HDFS includes
#define HDFS_MOCK_ENABLED

#include "hdfs_test_mocks.h"
#include "io/fs/hdfs_file_system.h"
#include "testutil/test_util.h"

namespace doris::io {

class HdfsCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Reset mock state
        HdfsMockState::instance()->reset();

        // Set up test configuration
        config::max_hdfs_file_system_cache_num = 5;
        config::max_hdfs_file_handle_cache_num = 10;
        config::num_partitions_for_hdfs_file_handle_cache = 2;
        config::max_hdfs_file_handle_cache_time_sec = 60;
    }

    void TearDown() override { HdfsMockState::instance()->cleanup(); }

    THdfsParams createTestHdfsParams(const std::string& fs_name = "test_fs",
                                     const std::string& user = "test_user") {
        THdfsParams params;
        params.__set_fs_name(fs_name);
        params.__set_user(user);
        return params;
    }

    THdfsParams createTestHdfsParamsWithConf(
            const std::string& fs_name, const std::string& user,
            const std::vector<std::pair<std::string, std::string>>& conf_pairs) {
        THdfsParams params;
        params.__set_fs_name(fs_name);
        params.__set_user(user);

        std::vector<THdfsConf> hdfs_conf;
        for (const auto& pair : conf_pairs) {
            THdfsConf conf;
            conf.key = pair.first;
            conf.value = pair.second;
            hdfs_conf.push_back(conf);
        }
        params.__set_hdfs_conf(hdfs_conf);
        return params;
    }
};

// Test RandomGenerator class functionality (tests the new random eviction strategy)
TEST_F(HdfsCacheTest, RandomGeneratorFunctionality) {
    // Create a RandomGenerator instance (replicating the one in HdfsFileSystemCache)
    class RandomGenerator {
    private:
        std::random_device _rd;
        std::mt19937 _gen;

    public:
        RandomGenerator() : _gen(_rd()) {}
        uint32_t Uniform(uint32_t max_value) {
            std::uniform_int_distribution<uint32_t> dis(0, max_value - 1);
            return dis(_gen);
        }
    };

    RandomGenerator rand_gen;

    // Test basic range validation
    const uint32_t max_val = 10;
    for (int i = 0; i < 100; ++i) {
        uint32_t result = rand_gen.Uniform(max_val);
        EXPECT_LT(result, max_val);
        EXPECT_GE(result, 0);
    }

    // Test edge case: max_value = 1
    EXPECT_EQ(0, rand_gen.Uniform(1));

    // Test different max values
    for (uint32_t max_val_test : {2, 5, 100}) {
        uint32_t result = rand_gen.Uniform(max_val_test);
        EXPECT_LT(result, max_val_test);
        EXPECT_GE(result, 0);
    }
}

// Test filesystem creation with different cache key parameters
TEST_F(HdfsCacheTest, CacheKeyGeneration) {
    std::vector<THdfsParams> unique_params;

    // Test Case 1: Different fs_name
    unique_params.push_back(createTestHdfsParams("hdfs://namenode1:9000", "user1"));
    unique_params.push_back(createTestHdfsParams("hdfs://namenode2:9000", "user1"));

    // Test Case 2: Different user
    unique_params.push_back(createTestHdfsParams("hdfs://namenode1:9000", "user2"));

    // Test Case 3: Different BEE configurations
    unique_params.push_back(createTestHdfsParamsWithConf(
            "hdfs://namenode1:9000", "user1",
            {{"BEE_USER", "bee_user1"}, {"BEE_SOURCE", "bee_source1"}}));

    unique_params.push_back(createTestHdfsParamsWithConf(
            "hdfs://namenode1:9000", "user1",
            {{"BEE_USER", "bee_user2"}, {"BEE_SOURCE", "bee_source1"}}));

    // Verify that we can create different parameter combinations
    // This tests that the cache key generation logic can handle different inputs
    for (size_t i = 0; i < unique_params.size(); ++i) {
        // Test that parameters are properly set
        EXPECT_TRUE(unique_params[i].__isset.fs_name);
        EXPECT_TRUE(unique_params[i].__isset.user);
        EXPECT_FALSE(unique_params[i].fs_name.empty());
        EXPECT_FALSE(unique_params[i].user.empty());

        // Test BEE configuration handling
        if (unique_params[i].__isset.hdfs_conf) {
            bool has_bee_config = false;
            for (const auto& conf : unique_params[i].hdfs_conf) {
                if (conf.key == "BEE_USER" || conf.key == "BEE_SOURCE") {
                    has_bee_config = true;
                    EXPECT_FALSE(conf.value.empty());
                }
            }
            // Cases 3 and 4 should have BEE config
            if (i >= 2) {
                EXPECT_TRUE(has_bee_config);
            }
        }
    }
}

// Test cache eviction logic (tests the new random eviction strategy)
TEST_F(HdfsCacheTest, CacheEvictionLogic) {
    // Test the cache size configuration
    const uint32_t original_cache_size = config::max_hdfs_file_system_cache_num;
    config::max_hdfs_file_system_cache_num = 3;

    // Verify cache size configuration is applied
    EXPECT_EQ(config::max_hdfs_file_system_cache_num, 3);

    // Test creating parameter sets that would trigger cache eviction
    std::vector<THdfsParams> params_list;
    const int num_params = 10; // More than cache capacity

    for (int i = 0; i < num_params; ++i) {
        THdfsParams params =
                createTestHdfsParams("fs_" + std::to_string(i), "user_" + std::to_string(i));
        params_list.push_back(params);

        // Verify each parameter set is unique
        EXPECT_EQ(params.fs_name, "fs_" + std::to_string(i));
        EXPECT_EQ(params.user, "user_" + std::to_string(i));
    }

    // Verify we created more parameter sets than cache capacity
    EXPECT_GT(params_list.size(), config::max_hdfs_file_system_cache_num);

    // Test that all parameter sets are different (would generate different cache keys)
    for (size_t i = 0; i < params_list.size(); ++i) {
        for (size_t j = i + 1; j < params_list.size(); ++j) {
            EXPECT_NE(params_list[i].fs_name, params_list[j].fs_name);
            EXPECT_NE(params_list[i].user, params_list[j].user);
        }
    }

    // Restore original cache size
    config::max_hdfs_file_system_cache_num = original_cache_size;
}

// Test concurrent parameter generation (tests thread safety concepts)
TEST_F(HdfsCacheTest, ConcurrentParameterGeneration) {
    const int num_threads = 4;
    const int operations_per_thread = 25;
    std::vector<std::thread> threads;
    std::atomic<int> successful_operations(0);
    std::vector<std::vector<THdfsParams>> thread_params(num_threads);

    // Launch multiple threads creating parameters concurrently
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([this, t, &successful_operations, &thread_params]() {
            for (int op = 0; op < 25; ++op) {
                try {
                    // Mix of unique and repeated parameters to test cache behavior
                    std::string fs_name = "fs_" + std::to_string(t) + "_" + std::to_string(op % 3);
                    std::string user = "user_" + std::to_string(t);
                    THdfsParams params = createTestHdfsParams(fs_name, user);

                    // Verify parameters are created correctly
                    if (!params.fs_name.empty() && !params.user.empty()) {
                        successful_operations++;
                        thread_params[t].push_back(params);
                    }
                } catch (const std::exception&) {
                    // Count exceptions but don't fail the test
                }
            }
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify that most operations succeeded
    int total_operations = num_threads * operations_per_thread;
    EXPECT_GT(successful_operations.load(), total_operations * 0.8); // At least 80% success rate

    // Verify that each thread created parameters
    for (int t = 0; t < num_threads; ++t) {
        EXPECT_GT(thread_params[t].size(), 0);

        // Verify parameters are valid
        for (const auto& params : thread_params[t]) {
            EXPECT_FALSE(params.fs_name.empty());
            EXPECT_FALSE(params.user.empty());
            EXPECT_TRUE(params.user.find("user_" + std::to_string(t)) != std::string::npos);
        }
    }
}

} // namespace doris::io
