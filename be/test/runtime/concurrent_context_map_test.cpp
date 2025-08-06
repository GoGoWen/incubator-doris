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

#include "runtime/fragment_mgr.h"

#include <gtest/gtest.h>
#include <memory>
#include <thread>
#include <vector>
#include <chrono>
#include <atomic>

#include "common/config.h"
#include "common/status.h"
#include "gen_cpp/Types_types.h"

namespace doris {

// Test value class for ConcurrentContextMap testing
class TestValue {
public:
    TestValue(int val) : value(val) {}
    int get_value() const { return value; }
    void set_value(int val) { value = val; }
private:
    int value;
};

// Test fixture for ConcurrentContextMap functionality
class ConcurrentContextMapTest : public testing::Test {
public:
    void SetUp() override {
        // Set up the configuration for num_query_ctx_map_partitions
        config::num_query_ctx_map_partitions = 128;
    }

    void TearDown() override {}

protected:
    // Helper to create TUniqueId
    TUniqueId create_unique_id(int64_t hi, int64_t lo) {
        TUniqueId id;
        id.__set_hi(hi);
        id.__set_lo(lo);
        return id;
    }
};

// Test hash functions
TEST_F(ConcurrentContextMapTest, TestHashFunctions) {
    TUniqueId id1 = create_unique_id(1, 2);
    TUniqueId id2 = create_unique_id(1, 2);
    TUniqueId id3 = create_unique_id(2, 3);
    
    size_t capacity = 128;
    
    uint32_t hash1 = get_map_id(id1, capacity);
    uint32_t hash2 = get_map_id(id2, capacity);
    uint32_t hash3 = get_map_id(id3, capacity);
    
    // Same IDs should produce same hash
    EXPECT_EQ(hash1, hash2);
    // Different IDs should likely produce different hashes
    EXPECT_NE(hash1, hash3);
    
    // Hash should be within capacity bounds
    EXPECT_LT(hash1, capacity);
    EXPECT_LT(hash2, capacity);
    EXPECT_LT(hash3, capacity);
}

// Test ConcurrentContextMap constructor and initialization
TEST_F(ConcurrentContextMapTest, TestConstructorAndInitialization) {
    ConcurrentContextMap<TUniqueId, std::shared_ptr<TestValue>, TestValue> map;
    
    // Map should be empty initially
    EXPECT_EQ(map.num_items(), 0);
    
    // Should be able to find nothing
    TUniqueId id = create_unique_id(1, 2);
    auto result = map.find(id);
    EXPECT_EQ(result, nullptr);
}

// Test ConcurrentContextMap insert and find methods
TEST_F(ConcurrentContextMapTest, TestInsertAndFind) {
    ConcurrentContextMap<TUniqueId, std::shared_ptr<TestValue>, TestValue> map;
    
    TUniqueId id1 = create_unique_id(1, 2);
    TUniqueId id2 = create_unique_id(3, 4);
    
    auto value1 = std::make_shared<TestValue>(100);
    auto value2 = std::make_shared<TestValue>(200);
    
    // Insert values
    map.insert(id1, value1);
    map.insert(id2, value2);
    
    // Should find inserted values
    auto found1 = map.find(id1);
    auto found2 = map.find(id2);
    
    EXPECT_NE(found1, nullptr);
    EXPECT_NE(found2, nullptr);
    EXPECT_EQ(found1->get_value(), 100);
    EXPECT_EQ(found2->get_value(), 200);
    
    // Should have 2 items
    EXPECT_EQ(map.num_items(), 2);
    
    // Should not find non-existent item
    TUniqueId id3 = create_unique_id(5, 6);
    auto found3 = map.find(id3);
    EXPECT_EQ(found3, nullptr);
}

// Test ConcurrentContextMap erase method
TEST_F(ConcurrentContextMapTest, TestErase) {
    ConcurrentContextMap<TUniqueId, std::shared_ptr<TestValue>, TestValue> map;
    
    TUniqueId id1 = create_unique_id(1, 2);
    TUniqueId id2 = create_unique_id(3, 4);
    
    auto value1 = std::make_shared<TestValue>(100);
    auto value2 = std::make_shared<TestValue>(200);
    
    // Insert values
    map.insert(id1, value1);
    map.insert(id2, value2);
    EXPECT_EQ(map.num_items(), 2);
    
    // Erase one value
    map.erase(id1);
    EXPECT_EQ(map.num_items(), 1);
    
    // Should not find erased value
    auto found1 = map.find(id1);
    EXPECT_EQ(found1, nullptr);
    
    // Should still find remaining value
    auto found2 = map.find(id2);
    EXPECT_NE(found2, nullptr);
    EXPECT_EQ(found2->get_value(), 200);
    
    // Erase remaining value
    map.erase(id2);
    EXPECT_EQ(map.num_items(), 0);
    
    // Should not find any values
    auto found2_after = map.find(id2);
    EXPECT_EQ(found2_after, nullptr);
}

// Test ConcurrentContextMap clear method
TEST_F(ConcurrentContextMapTest, TestClear) {
    ConcurrentContextMap<TUniqueId, std::shared_ptr<TestValue>, TestValue> map;
    
    // Insert multiple values
    for (int i = 0; i < 10; ++i) {
        TUniqueId id = create_unique_id(i, i + 100);
        auto value = std::make_shared<TestValue>(i * 10);
        map.insert(id, value);
    }
    
    EXPECT_EQ(map.num_items(), 10);
    
    // Clear all values
    map.clear();
    EXPECT_EQ(map.num_items(), 0);
    
    // Should not find any values
    for (int i = 0; i < 10; ++i) {
        TUniqueId id = create_unique_id(i, i + 100);
        auto found = map.find(id);
        EXPECT_EQ(found, nullptr);
    }
}

// Test ConcurrentContextMap apply_if_not_exists method
TEST_F(ConcurrentContextMapTest, TestApplyIfNotExists) {
    ConcurrentContextMap<TUniqueId, std::shared_ptr<TestValue>, TestValue> map;
    
    TUniqueId id1 = create_unique_id(1, 2);
    std::shared_ptr<TestValue> query_ctx;
    
    // First call should execute the function
    bool function_called = false;
    Status result = map.apply_if_not_exists(id1, query_ctx, 
        [&](phmap::flat_hash_map<TUniqueId, std::shared_ptr<TestValue>>& internal_map) -> Status {
            function_called = true;
            auto new_value = std::make_shared<TestValue>(500);
            internal_map.insert({id1, new_value});
            query_ctx = new_value;
            return Status::OK();
        });
    
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(function_called);
    EXPECT_NE(query_ctx, nullptr);
    EXPECT_EQ(query_ctx->get_value(), 500);
    EXPECT_EQ(map.num_items(), 1);
    
    // Second call should not execute the function (item exists)
    std::shared_ptr<TestValue> query_ctx2;
    bool function_called2 = false;
    Status result2 = map.apply_if_not_exists(id1, query_ctx2,
        [&](phmap::flat_hash_map<TUniqueId, std::shared_ptr<TestValue>>& internal_map) -> Status {
            function_called2 = true;
            return Status::OK();
        });
    
    EXPECT_TRUE(result2.ok());
    EXPECT_FALSE(function_called2);
    EXPECT_NE(query_ctx2, nullptr);
    EXPECT_EQ(query_ctx2->get_value(), 500);
    EXPECT_EQ(map.num_items(), 1);
}

// Test apply method
TEST_F(ConcurrentContextMapTest, TestApply) {
    ConcurrentContextMap<TUniqueId, std::shared_ptr<TestValue>, TestValue> map;
    
    // Insert test data
    const int num_items = 50;
    for (int i = 0; i < num_items; ++i) {
        TUniqueId id = create_unique_id(i, i * 2);
        auto value = std::make_shared<TestValue>(i);
        map.insert(id, value);
    }
    
    EXPECT_EQ(map.num_items(), num_items);
    
    // Use apply to modify all values
    std::atomic<int> processed_count{0};
    map.apply([&](phmap::flat_hash_map<TUniqueId, std::shared_ptr<TestValue>>& internal_map) -> Status {
        for (auto& pair : internal_map) {
            pair.second->set_value(pair.second->get_value() * 2);
            processed_count++;
        }
        return Status::OK();
    });
    
    EXPECT_EQ(processed_count.load(), num_items);
    
    // Verify all values were modified
    for (int i = 0; i < num_items; ++i) {
        TUniqueId id = create_unique_id(i, i * 2);
        auto found = map.find(id);
        EXPECT_NE(found, nullptr);
        EXPECT_EQ(found->get_value(), i * 2);
    }
}

// Test thread safety with concurrent operations
TEST_F(ConcurrentContextMapTest, TestConcurrency) {
    ConcurrentContextMap<TUniqueId, std::shared_ptr<TestValue>, TestValue> map;
    
    const int num_threads = 10;
    const int operations_per_thread = 100;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    
    // Launch threads that perform concurrent insertions
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < operations_per_thread; ++i) {
                TUniqueId id = create_unique_id(t * operations_per_thread + i, t);
                auto value = std::make_shared<TestValue>(t * 1000 + i);
                
                map.insert(id, value);
                
                // Verify we can find the inserted value
                auto found = map.find(id);
                if (found && found->get_value() == t * 1000 + i) {
                    success_count++;
                }
            }
        });
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    // All operations should have succeeded
    EXPECT_EQ(success_count.load(), num_threads * operations_per_thread);
    EXPECT_EQ(map.num_items(), num_threads * operations_per_thread);
}

// Test concurrent find and erase operations
TEST_F(ConcurrentContextMapTest, TestConcurrentFindAndErase) {
    ConcurrentContextMap<TUniqueId, std::shared_ptr<TestValue>, TestValue> map;
    
    // Pre-populate the map
    const int initial_items = 1000;
    for (int i = 0; i < initial_items; ++i) {
        TUniqueId id = create_unique_id(i, i * 2);
        auto value = std::make_shared<TestValue>(i * 10);
        map.insert(id, value);
    }
    
    EXPECT_EQ(map.num_items(), initial_items);
    
    const int num_threads = 8;
    std::vector<std::thread> threads;
    std::atomic<int> find_success{0};
    std::atomic<int> erase_count{0};
    
    // Launch threads that perform concurrent finds and erases
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = t; i < initial_items; i += num_threads) {
                TUniqueId id = create_unique_id(i, i * 2);
                
                // Try to find
                auto found = map.find(id);
                if (found) {
                    find_success++;
                }
                
                // Try to erase every other item
                if (i % 2 == 0) {
                    map.erase(id);
                    erase_count++;
                }
            }
        });
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    // We expect roughly half the items to be erased
    size_t expected_remaining = initial_items - erase_count.load();
    EXPECT_EQ(map.num_items(), expected_remaining);
    
    // Verify that erased items cannot be found
    int found_erased_items = 0;
    for (int i = 0; i < initial_items; i += 2) {
        TUniqueId id = create_unique_id(i, i * 2);
        auto found = map.find(id);
        if (found) {
            found_erased_items++;
        }
    }
    EXPECT_EQ(found_erased_items, 0);
}

// Test apply_if_not_exists with concurrent access
TEST_F(ConcurrentContextMapTest, TestConcurrentApplyIfNotExists) {
    ConcurrentContextMap<TUniqueId, std::shared_ptr<TestValue>, TestValue> map;
    
    const int num_threads = 10;
    TUniqueId shared_id = create_unique_id(999, 888);
    std::vector<std::thread> threads;
    std::atomic<int> function_call_count{0};
    std::vector<std::shared_ptr<TestValue>> results(num_threads);
    
    // Launch threads that all try to create the same item
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::shared_ptr<TestValue> query_ctx;
            Status result = map.apply_if_not_exists(shared_id, query_ctx,
                [&](phmap::flat_hash_map<TUniqueId, std::shared_ptr<TestValue>>& internal_map) -> Status {
                    function_call_count++;
                    auto new_value = std::make_shared<TestValue>(12345);
                    internal_map.insert({shared_id, new_value});
                    query_ctx = new_value;
                    return Status::OK();
                });
            
            EXPECT_TRUE(result.ok());
            results[t] = query_ctx;
        });
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Only one thread should have called the function
    EXPECT_EQ(function_call_count.load(), 1);
    EXPECT_EQ(map.num_items(), 1);
    
    // All threads should have gotten the same result
    for (int t = 0; t < num_threads; ++t) {
        EXPECT_NE(results[t], nullptr);
        EXPECT_EQ(results[t]->get_value(), 12345);
    }
}

// Performance test to validate partitioning efficiency
TEST_F(ConcurrentContextMapTest, TestPartitioningEfficiency) {
    ConcurrentContextMap<TUniqueId, std::shared_ptr<TestValue>, TestValue> map;
    
    const int num_items = 10000;
    
    // Insert many items
    for (int i = 0; i < num_items; ++i) {
        TUniqueId id = create_unique_id(i, i * 3 + 7);
        auto value = std::make_shared<TestValue>(i);
        map.insert(id, value);
    }
    
    EXPECT_EQ(map.num_items(), num_items);
    
    // Measure performance of finds
    auto start_time = std::chrono::high_resolution_clock::now();
    
    int found_count = 0;
    for (int i = 0; i < num_items; ++i) {
        TUniqueId id = create_unique_id(i, i * 3 + 7);
        auto found = map.find(id);
        if (found && found->get_value() == i) {
            found_count++;
        }
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    
    EXPECT_EQ(found_count, num_items);
    
    // Performance should be reasonable (this is a rough check)
    // With partitioning, lookups should be fast even with many items
    EXPECT_LT(duration.count(), 100000); // Less than 100ms for 10k lookups
}

// Test configuration parameter validation
TEST_F(ConcurrentContextMapTest, TestConfigurationParameter) {
    // Test with different partition counts
    int original_partitions = config::num_query_ctx_map_partitions;
    
    // Test with smaller partition count
    config::num_query_ctx_map_partitions = 4;
    {
        ConcurrentContextMap<TUniqueId, std::shared_ptr<TestValue>, TestValue> map;
        
        TUniqueId id = create_unique_id(1, 2);
        auto value = std::make_shared<TestValue>(42);
        map.insert(id, value);
        
        auto found = map.find(id);
        EXPECT_NE(found, nullptr);
        EXPECT_EQ(found->get_value(), 42);
    }
    
    // Test with larger partition count
    config::num_query_ctx_map_partitions = 256;
    {
        ConcurrentContextMap<TUniqueId, std::shared_ptr<TestValue>, TestValue> map;
        
        TUniqueId id = create_unique_id(1, 2);
        auto value = std::make_shared<TestValue>(84);
        map.insert(id, value);
        
        auto found = map.find(id);
        EXPECT_NE(found, nullptr);
        EXPECT_EQ(found->get_value(), 84);
    }
    
    // Restore original value
    config::num_query_ctx_map_partitions = original_partitions;
}

// Test hash distribution across partitions
TEST_F(ConcurrentContextMapTest, TestHashDistribution) {
    const int num_partitions = 16;
    config::num_query_ctx_map_partitions = num_partitions;
    
    ConcurrentContextMap<TUniqueId, std::shared_ptr<TestValue>, TestValue> map;
    
    // Insert items and track which partitions they go to
    const int num_items = 1000;
    std::vector<int> partition_counts(num_partitions, 0);
    
    for (int i = 0; i < num_items; ++i) {
        TUniqueId id = create_unique_id(i, i * 7 + 13); // Use varied values
        auto value = std::make_shared<TestValue>(i);
        map.insert(id, value);
        
        // Calculate which partition this item went to
        uint32_t hash_val = get_map_id(id, num_partitions);
        partition_counts[hash_val]++;
    }
    
    EXPECT_EQ(map.num_items(), num_items);
    
    // Check that items are reasonably distributed across partitions
    // With good hashing, no partition should be empty or overly full
    for (int i = 0; i < num_partitions; ++i) {
        EXPECT_GT(partition_counts[i], 0); // No partition should be empty
        EXPECT_LT(partition_counts[i], num_items / 2); // No partition should have more than half
    }
    
    // Verify all items can still be found
    for (int i = 0; i < num_items; ++i) {
        TUniqueId id = create_unique_id(i, i * 7 + 13);
        auto found = map.find(id);
        EXPECT_NE(found, nullptr);
        EXPECT_EQ(found->get_value(), i);
    }
}

} // namespace doris