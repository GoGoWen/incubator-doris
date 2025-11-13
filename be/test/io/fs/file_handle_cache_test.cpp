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

#include <atomic>
#include <barrier>
#include <condition_variable>
#include <functional>
#include <latch>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"

#define HDFS_MOCK_ENABLED

#include "hdfs_test_mocks.h"
#include "io/fs/file_handle_cache.h"
#include "io/fs/hdfs_file_system.h"
#include "testutil/test_util.h"

namespace doris::io {

class FileHandleCacheKeyTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

class FileHandleCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        HdfsMockState::instance()->reset();

        config::max_hdfs_file_handle_cache_num = 10;
        config::num_partitions_for_hdfs_file_handle_cache = 2;
        config::max_hdfs_file_handle_cache_time_sec = 60;
    }

    void TearDown() override {
        HdfsMockState::instance()->cleanup();
    }

    // Helper to create audit context for testing
    hdfsAuditContext createAuditContext(const char* erp, const char* source) {
        hdfsAuditContext ctx;
        ctx.erp = erp;
        ctx.source = source;
        ctx.businessId = nullptr;
        return ctx;
    }
};

class HdfsFileSystemCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        HdfsMockState::instance()->reset();
        config::max_hdfs_file_system_cache_num = 5;
    }

    void TearDown() override {
        HdfsMockState::instance()->cleanup();
    }

    THdfsParams createTestHdfsParams(const std::string& fs_name = "test_fs",
                                     const std::string& user = "test_user",
                                     const std::string& bee_user = "",
                                     const std::string& bee_source = "") {
        THdfsParams params;
        params.__set_fs_name(fs_name);
        params.__set_user(user);

        if (!bee_user.empty() || !bee_source.empty()) {
            std::vector<THdfsConf> hdfs_conf;
            if (!bee_user.empty()) {
                THdfsConf conf;
                conf.key = "BEE_USER";
                conf.value = bee_user;
                hdfs_conf.push_back(conf);
            }
            if (!bee_source.empty()) {
                THdfsConf conf;
                conf.key = "BEE_SOURCE";
                conf.value = bee_source;
                hdfs_conf.push_back(conf);
            }
            params.__set_hdfs_conf(hdfs_conf);
        }

        return params;
    }
};

TEST_F(FileHandleCacheKeyTest, FileHandleCacheKeyEquality) {
    FileHandleCacheKey key1("user1", "file1", 12345, "bee_user1", "bee_source1");
    FileHandleCacheKey key2("user1", "file1", 12345, "bee_user1", "bee_source1");

    EXPECT_EQ(key1, key2);
}

TEST_F(FileHandleCacheKeyTest, FileHandleCacheKeyInequalityUser) {
    FileHandleCacheKey key1("user1", "file1", 12345, "bee_user1", "bee_source1");
    FileHandleCacheKey key2("user2", "file1", 12345, "bee_user1", "bee_source1");

    EXPECT_NE(key1, key2);
}

TEST_F(FileHandleCacheKeyTest, FileHandleCacheKeyInequalityFile) {
    FileHandleCacheKey key1("user1", "file1", 12345, "bee_user1", "bee_source1");
    FileHandleCacheKey key2("user1", "file2", 12345, "bee_user1", "bee_source1");

    EXPECT_NE(key1, key2);
}

TEST_F(FileHandleCacheKeyTest, FileHandleCacheKeyInequalityMtime) {
    FileHandleCacheKey key1("user1", "file1", 12345, "bee_user1", "bee_source1");
    FileHandleCacheKey key2("user1", "file1", 67890, "bee_user1", "bee_source1");

    EXPECT_NE(key1, key2);
}

TEST_F(FileHandleCacheKeyTest, FileHandleCacheKeyInequalityBeeUser) {
    FileHandleCacheKey key1("user1", "file1", 12345, "bee_user1", "bee_source1");
    FileHandleCacheKey key2("user1", "file1", 12345, "bee_user2", "bee_source1");

    EXPECT_NE(key1, key2);
}

TEST_F(FileHandleCacheKeyTest, FileHandleCacheKeyInequalityBeeSource) {
    FileHandleCacheKey key1("user1", "file1", 12345, "bee_user1", "bee_source1");
    FileHandleCacheKey key2("user1", "file1", 12345, "bee_user1", "bee_source2");

    EXPECT_NE(key1, key2);
}

TEST_F(FileHandleCacheKeyTest, FileHandleCacheKeyHashConsistency) {
    std::hash<FileHandleCacheKey> hasher;

    FileHandleCacheKey key1("user1", "file1", 12345, "bee_user1", "bee_source1");
    FileHandleCacheKey key2("user1", "file1", 12345, "bee_user1", "bee_source1");

    EXPECT_EQ(hasher(key1), hasher(key2));
}

TEST_F(FileHandleCacheKeyTest, FileHandleCacheKeyHashDiversityBeeUser) {
    std::hash<FileHandleCacheKey> hasher;

    FileHandleCacheKey key1("user1", "file1", 12345, "bee_user1", "bee_source1");
    FileHandleCacheKey key2("user1", "file1", 12345, "bee_user2", "bee_source1");

    EXPECT_NE(hasher(key1), hasher(key2));
}

TEST_F(FileHandleCacheKeyTest, FileHandleCacheKeyHashDiversityBeeSource) {
    std::hash<FileHandleCacheKey> hasher;

    FileHandleCacheKey key1("user1", "file1", 12345, "bee_user1", "bee_source1");
    FileHandleCacheKey key2("user1", "file1", 12345, "bee_user1", "bee_source2");

    EXPECT_NE(hasher(key1), hasher(key2));
}

TEST_F(FileHandleCacheKeyTest, FileHandleCacheKeyBackwardCompatibilityEmptyBeeFields) {
    FileHandleCacheKey key1("user1", "file1", 12345, "", "");
    FileHandleCacheKey key2("user1", "file1", 12345);

    EXPECT_EQ(key1, key2);

    std::hash<FileHandleCacheKey> hasher;
    EXPECT_EQ(hasher(key1), hasher(key2));
}

TEST_F(FileHandleCacheKeyTest, FileHandleCacheKeyBackwardCompatibilityPartialBeeFields) {
    FileHandleCacheKey key_with_bee_user("user1", "file1", 12345, "bee_user1", "");
    FileHandleCacheKey key_with_bee_source("user1", "file1", 12345, "", "bee_source1");
    FileHandleCacheKey key_without_bee("user1", "file1", 12345, "", "");

    EXPECT_NE(key_with_bee_user, key_without_bee);
    EXPECT_NE(key_with_bee_source, key_without_bee);
    EXPECT_NE(key_with_bee_user, key_with_bee_source);
}

TEST_F(FileHandleCacheKeyTest, FileHandleCacheKeyHashDistribution) {
    std::hash<FileHandleCacheKey> hasher;
    std::unordered_set<size_t> hashes;

    // Generate 100 different keys and verify hash diversity
    for (int i = 0; i < 100; ++i) {
        std::string bee_user = "bee_user_" + std::to_string(i);
        std::string bee_source = "bee_source_" + std::to_string(i % 10);
        FileHandleCacheKey key("user1", "file1", 12345, bee_user, bee_source);
        hashes.insert(hasher(key));
    }

    // Expect high diversity (at least 95% unique hashes)
    EXPECT_GT(hashes.size(), 95);
}

TEST_F(FileHandleCacheTest, FileHandleCacheKeyIncludesAuditContext) {
    // Verify that cache keys include audit context fields
    hdfsAuditContext audit_ctx = createAuditContext("test_erp", "test_source");

    // Create cache key as it would be created in file_handle_cache.cpp:163
    FileHandleCacheKey key("user1", "/test/file.txt", 12345,
                           audit_ctx.erp, audit_ctx.source);

    EXPECT_EQ(key.user, "user1");
    EXPECT_EQ(key.fname, "/test/file.txt");
    EXPECT_EQ(key.mtime, 12345);
    EXPECT_EQ(key.bee_user, "test_erp");
    EXPECT_EQ(key.bee_source, "test_source");
}

TEST_F(FileHandleCacheTest, DifferentAuditContextProducesDifferentKey) {
    // Different erp values should produce different cache keys
    hdfsAuditContext audit_ctx1 = createAuditContext("erp1", "source1");
    hdfsAuditContext audit_ctx2 = createAuditContext("erp2", "source1");

    FileHandleCacheKey key1("user1", "/test/file.txt", 12345,
                            audit_ctx1.erp, audit_ctx1.source);
    FileHandleCacheKey key2("user1", "/test/file.txt", 12345,
                            audit_ctx2.erp, audit_ctx2.source);

    EXPECT_NE(key1, key2);

    std::hash<FileHandleCacheKey> hasher;
    EXPECT_NE(hasher(key1), hasher(key2));
}

TEST_F(FileHandleCacheTest, SameAuditContextProducesSameKey) {
    // Same audit context should produce identical cache keys
    hdfsAuditContext audit_ctx1 = createAuditContext("erp1", "source1");
    hdfsAuditContext audit_ctx2 = createAuditContext("erp1", "source1");

    FileHandleCacheKey key1("user1", "/test/file.txt", 12345,
                            audit_ctx1.erp, audit_ctx1.source);
    FileHandleCacheKey key2("user1", "/test/file.txt", 12345,
                            audit_ctx2.erp, audit_ctx2.source);

    EXPECT_EQ(key1, key2);

    std::hash<FileHandleCacheKey> hasher;
    EXPECT_EQ(hasher(key1), hasher(key2));
}

TEST_F(FileHandleCacheTest, DifferentSourceProducesDifferentKey) {
    // Different source values should produce different cache keys
    hdfsAuditContext audit_ctx1 = createAuditContext("erp1", "source1");
    hdfsAuditContext audit_ctx2 = createAuditContext("erp1", "source2");

    FileHandleCacheKey key1("user1", "/test/file.txt", 12345,
                            audit_ctx1.erp, audit_ctx1.source);
    FileHandleCacheKey key2("user1", "/test/file.txt", 12345,
                            audit_ctx2.erp, audit_ctx2.source);

    EXPECT_NE(key1, key2);

    std::hash<FileHandleCacheKey> hasher;
    EXPECT_NE(hasher(key1), hasher(key2));
}

TEST_F(FileHandleCacheTest, AuditContextNullPointerSafety) {
    // In actual usage (file_handle_cache.cpp:163), audit_context->erp and
    // audit_context->source are const char* that might be nullptr.
    // The FileHandleCacheKey constructor converts them to std::string.

    // Simulate what happens when audit_context fields are nullptr
    const char* null_erp = nullptr;
    const char* null_source = nullptr;

    // When converting nullptr const char* to string, we need to handle it
    std::string erp_str = null_erp ? null_erp : "";
    std::string source_str = null_source ? null_source : "";

    FileHandleCacheKey key_with_values("user1", "/test/file.txt", 12345,
                                       "erp1", "source1");
    FileHandleCacheKey key_with_safe_nulls("user1", "/test/file.txt", 12345,
                                           erp_str, source_str);

    // Keys with values vs empty (from null) should be different
    EXPECT_NE(key_with_values, key_with_safe_nulls);

    // The empty-from-null key should equal a key with explicit empty strings
    FileHandleCacheKey key_with_empty("user1", "/test/file.txt", 12345, "", "");
    EXPECT_EQ(key_with_safe_nulls, key_with_empty);
}

TEST_F(FileHandleCacheTest, AuditContextEmptyStringHandling) {
    // Test with empty strings (backward compatibility)
    FileHandleCacheKey key_with_values("user1", "/test/file.txt", 12345,
                                       "erp1", "source1");
    FileHandleCacheKey key_with_empty("user1", "/test/file.txt", 12345,
                                      "", "");

    // Keys with values vs empty strings should be different
    EXPECT_NE(key_with_values, key_with_empty);
}

TEST_F(HdfsFileSystemCacheTest, ConcurrentParameterValidation) {
    // Verify that multiple threads can generate valid parameters concurrently
    const int num_threads = 10;
    std::vector<std::thread> threads;
    std::vector<THdfsParams> results(num_threads);
    std::barrier sync_point(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            sync_point.arrive_and_wait();

            // Generate parameters concurrently
            results[i] = createTestHdfsParams(
                "hdfs://namenode:9000",
                "user1",
                "bee_user_" + std::to_string(i % 3),
                "bee_source_" + std::to_string(i % 2)
            );
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all parameters are valid
    for (const auto& params : results) {
        EXPECT_TRUE(params.__isset.fs_name);
        EXPECT_TRUE(params.__isset.user);
        EXPECT_EQ(params.fs_name, "hdfs://namenode:9000");
        EXPECT_EQ(params.user, "user1");
    }
}

TEST_F(HdfsFileSystemCacheTest, UniqueParameterGeneration) {
    // Test that different audit contexts produce different parameter sets
    std::vector<THdfsParams> params_list;

    for (int i = 0; i < 5; ++i) {
        std::string bee_user = "bee_user_" + std::to_string(i);
        std::string bee_source = "bee_source_" + std::to_string(i);

        THdfsParams params = createTestHdfsParams("hdfs://namenode:9000", "user1",
                                                   bee_user, bee_source);
        params_list.push_back(params);
    }

    // Verify all params have correct structure
    for (const auto& params : params_list) {
        EXPECT_TRUE(params.__isset.fs_name);
        EXPECT_TRUE(params.__isset.hdfs_conf);
        EXPECT_EQ(params.hdfs_conf.size(), 2);  // BEE_USER and BEE_SOURCE
    }

    // Verify they're different
    for (size_t i = 0; i < params_list.size(); ++i) {
        for (size_t j = i + 1; j < params_list.size(); ++j) {
            bool has_different_config = false;
            for (const auto& conf : params_list[i].hdfs_conf) {
                for (const auto& conf2 : params_list[j].hdfs_conf) {
                    if (conf.key == conf2.key && conf.value != conf2.value) {
                        has_different_config = true;
                        break;
                    }
                }
            }
            EXPECT_TRUE(has_different_config);
        }
    }
}

TEST_F(HdfsFileSystemCacheTest, CacheConfigValidation) {
    // Test cache size configuration
    const uint32_t original_cache_size = config::max_hdfs_file_system_cache_num;

    // Verify cache configuration is readable
    EXPECT_GT(config::max_hdfs_file_system_cache_num, 0);

    // Test with different cache sizes
    std::vector<uint32_t> test_sizes = {1, 3, 5, 10};
    for (uint32_t size : test_sizes) {
        config::max_hdfs_file_system_cache_num = size;
        EXPECT_EQ(config::max_hdfs_file_system_cache_num, size);
    }

    // Restore original
    config::max_hdfs_file_system_cache_num = original_cache_size;
}

TEST_F(HdfsFileSystemCacheTest, AuditContextConfigurationVariations) {
    // Test various audit context configurations
    std::vector<std::tuple<std::string, std::string, std::string>> test_cases = {
        {"erp1", "source1", ""},
        {"erp1", "", "source1"},
        {"", "source1", "source2"},
        {"erp1", "source1", "source2"},
    };

    for (const auto& [bee_user, bee_source1, bee_source2] : test_cases) {
        THdfsParams params = createTestHdfsParams("hdfs://namenode:9000", "user1",
                                                   bee_user, bee_source1);

        EXPECT_TRUE(params.__isset.fs_name);
        EXPECT_EQ(params.fs_name, "hdfs://namenode:9000");

        if (!bee_user.empty() || !bee_source1.empty()) {
            EXPECT_TRUE(params.__isset.hdfs_conf);
        }
    }
}

TEST_F(HdfsFileSystemCacheTest, ParameterIsolation) {
    // Verify that parameters created concurrently don't interfere
    const int num_params = 20;
    std::vector<THdfsParams> params_vec;

    for (int i = 0; i < num_params; ++i) {
        std::string fs_name = "hdfs://namenode" + std::to_string(i) + ":9000";
        std::string user = "user_" + std::to_string(i);
        std::string bee_user = "bee_user_" + std::to_string(i);
        std::string bee_source = "bee_source_" + std::to_string(i);

        THdfsParams params = createTestHdfsParams(fs_name, user, bee_user, bee_source);
        params_vec.push_back(params);
    }

    // Verify each parameter set is independent
    for (int i = 0; i < num_params; ++i) {
        EXPECT_EQ(params_vec[i].fs_name, "hdfs://namenode" + std::to_string(i) + ":9000");
        EXPECT_EQ(params_vec[i].user, "user_" + std::to_string(i));

        if (params_vec[i].__isset.hdfs_conf) {
            for (const auto& conf : params_vec[i].hdfs_conf) {
                if (conf.key == "BEE_USER") {
                    EXPECT_EQ(conf.value, "bee_user_" + std::to_string(i));
                } else if (conf.key == "BEE_SOURCE") {
                    EXPECT_EQ(conf.value, "bee_source_" + std::to_string(i));
                }
            }
        }
    }
}

} // namespace doris::io
