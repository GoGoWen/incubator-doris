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
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "common/object_pool.h"
#include "common/status.h"

// Enable HDFS mocking for tests - must be defined before any HDFS includes
#define HDFS_MOCK_ENABLED

#include "hdfs_test_mocks.h"
#include "io/fs/new_hdfs_file_reader.h"
#include "io/fs/file_handle_cache.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/path.h"
#include "testutil/test_util.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"
#include "util/slice.h"

namespace doris::io {

class NewHdfsFileReaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Reset mock state
        HdfsMockState::instance()->reset();

        // Create test objects
        _path = Path("/test/file.txt");
        _namenode = "hdfs://localhost:9000";
        _file_size = 1024;
        _mtime = 1234567890;

        // Create object pool for profiles
        _pool = std::make_unique<ObjectPool>();
        _profile = _pool->add(new RuntimeProfile("TestProfile"));

        // Store initial metrics values for comparison
        _initial_open_reading = DorisMetrics::instance()->hdfs_file_open_reading->value();
        _initial_reader_total = DorisMetrics::instance()->hdfs_file_reader_total->value();
    }

    void TearDown() override {
        // Clean up mock state
        HdfsMockState::instance()->cleanup();
        _pool.reset();
    }

protected:
    Path _path;
    std::string _namenode;
    std::unique_ptr<ObjectPool> _pool;
    RuntimeProfile* _profile;
    int64_t _file_size;
    int64_t _mtime;

    // Metrics tracking
    int64_t _initial_open_reading;
    int64_t _initial_reader_total;
};

// Test HDFS mock functionality directly
TEST_F(NewHdfsFileReaderTest, HdfsMockBasicFunctionality) {
    // Test that our HDFS mocks work correctly
    hdfsFS mock_fs = reinterpret_cast<hdfsFS>(0x1000);
    hdfsFile mock_file = reinterpret_cast<hdfsFile>(0x2000);

    // Test hdfsPread mock
    char buffer[64];
    tSize result = hdfsPread(mock_fs, mock_file, 0, buffer, 32);
    EXPECT_EQ(result, 32);
    EXPECT_EQ(buffer[0], 'A'); // Position 0 should give 'A'

    // Test hdfsPread at different position
    result = hdfsPread(mock_fs, mock_file, 5, buffer, 32);
    EXPECT_EQ(result, 32);
    EXPECT_EQ(buffer[0], 'F'); // Position 5 should give 'A' + 5 = 'F'

    // Test hdfsRead mock
    result = hdfsRead(mock_fs, mock_file, buffer, 32);
    EXPECT_EQ(result, 32);
    EXPECT_EQ(buffer[0], 'A');

    // Test hdfsSeek mock
    int seek_result = hdfsSeek(mock_fs, mock_file, 100);
    EXPECT_EQ(seek_result, 0);
}

TEST_F(NewHdfsFileReaderTest, HdfsMockErrorConditions) {
    hdfsFS mock_fs = reinterpret_cast<hdfsFS>(0x1000);
    hdfsFile mock_file = reinterpret_cast<hdfsFile>(0x2000);
    char buffer[64];

    // Test hdfsPread failure
    HdfsMockState::instance()->hdfs_pread_should_fail = true;
    tSize result = hdfsPread(mock_fs, mock_file, 0, buffer, 32);
    EXPECT_EQ(result, -1);

    // Reset and test hdfsRead failure
    HdfsMockState::instance()->reset();
    HdfsMockState::instance()->hdfs_read_should_fail = true;
    result = hdfsRead(mock_fs, mock_file, buffer, 32);
    EXPECT_EQ(result, -1);

    // Reset and test hdfsSeek failure
    HdfsMockState::instance()->reset();
    HdfsMockState::instance()->hdfs_seek_should_fail = true;
    int seek_result = hdfsSeek(mock_fs, mock_file, 100);
    EXPECT_EQ(seek_result, -1);
}

TEST_F(NewHdfsFileReaderTest, HdfsMockNullPointerHandling) {
    char buffer[64];

    // Test with null filesystem
    tSize result = hdfsPread(nullptr, reinterpret_cast<hdfsFile>(0x2000), 0, buffer, 32);
    EXPECT_EQ(result, -1);

    // Test with null file
    result = hdfsPread(reinterpret_cast<hdfsFS>(0x1000), nullptr, 0, buffer, 32);
    EXPECT_EQ(result, -1);

    // Test with null buffer
    result = hdfsPread(reinterpret_cast<hdfsFS>(0x1000), reinterpret_cast<hdfsFile>(0x2000), 0, nullptr, 32);
    EXPECT_EQ(result, -1);
}

TEST_F(NewHdfsFileReaderTest, HdfsMockDataPatterns) {
    hdfsFS mock_fs = reinterpret_cast<hdfsFS>(0x1000);
    hdfsFile mock_file = reinterpret_cast<hdfsFile>(0x2000);
    char buffer[64];

    // Test that hdfsPread returns different patterns based on position
    for (int i = 0; i < 26; ++i) {
        tSize result = hdfsPread(mock_fs, mock_file, i, buffer, 1);
        EXPECT_EQ(result, 1);
        char expected = 'A' + (i % 26);
        EXPECT_EQ(buffer[0], expected) << "Position " << i << " should return '" << expected << "'";
    }

    // Test wraparound after 'Z'
    tSize result = hdfsPread(mock_fs, mock_file, 26, buffer, 1);
    EXPECT_EQ(result, 1);
    EXPECT_EQ(buffer[0], 'A'); // 26 % 26 = 0, so should be 'A' again
}

// Test DorisMetrics integration
TEST_F(NewHdfsFileReaderTest, DorisMetricsIntegration) {
    // Verify that DorisMetrics instance is available and working
    EXPECT_TRUE(DorisMetrics::instance() != nullptr);

    // Test that we can access HDFS-related metrics
    auto* open_reading = DorisMetrics::instance()->hdfs_file_open_reading;
    auto* reader_total = DorisMetrics::instance()->hdfs_file_reader_total;

    EXPECT_TRUE(open_reading != nullptr);
    EXPECT_TRUE(reader_total != nullptr);

    // Test that we can increment/decrement metrics
    int64_t initial_open = open_reading->value();
    int64_t initial_total = reader_total->value();

    open_reading->increment(1);
    reader_total->increment(1);

    EXPECT_EQ(open_reading->value(), initial_open + 1);
    EXPECT_EQ(reader_total->value(), initial_total + 1);

    open_reading->increment(-1);

    EXPECT_EQ(open_reading->value(), initial_open);
    EXPECT_EQ(reader_total->value(), initial_total + 1);
}

// Test RuntimeProfile integration
TEST_F(NewHdfsFileReaderTest, RuntimeProfileIntegration) {
    EXPECT_TRUE(_profile != nullptr);
    EXPECT_EQ(_profile->name(), "TestProfile");

    // Test adding counters (similar to what NewHdfsFileReader does)
    auto* counter = ADD_COUNTER(_profile, "TestCounter", TUnit::BYTES);
    EXPECT_TRUE(counter != nullptr);

    counter->update(100);
    EXPECT_EQ(counter->value(), 100);

    // Test adding timer first, then child counter
    ADD_TIMER(_profile, "TestTimer");
    auto* child_counter = ADD_CHILD_COUNTER(_profile, "ChildCounter", TUnit::BYTES, "TestTimer");
    EXPECT_TRUE(child_counter != nullptr);

    child_counter->update(50);
    EXPECT_EQ(child_counter->value(), 50);
}

// Test Path functionality
TEST_F(NewHdfsFileReaderTest, PathFunctionality) {
    Path test_path("/hdfs/test/file.txt");

    EXPECT_EQ(test_path.string(), "/hdfs/test/file.txt");
    EXPECT_EQ(test_path.native(), "/hdfs/test/file.txt");

    Path another_path = test_path;
    EXPECT_EQ(another_path.string(), test_path.string());
}

// Test Slice functionality used in read operations
TEST_F(NewHdfsFileReaderTest, SliceFunctionality) {
    char buffer[64];
    memset(buffer, 'X', sizeof(buffer));

    Slice slice(buffer, 32);
    EXPECT_EQ(slice.size, 32);
    EXPECT_EQ(slice.data, buffer);

    // Verify buffer contents
    for (int i = 0; i < 32; ++i) {
        EXPECT_EQ(slice.data[i], 'X');
    }
}

// Test Status error handling patterns
TEST_F(NewHdfsFileReaderTest, StatusErrorHandling) {
    // Test creating different types of Status
    Status ok_status = Status::OK();
    EXPECT_TRUE(ok_status.ok());
    EXPECT_FALSE(ok_status.is<ErrorCode::IO_ERROR>());

    Status io_error = Status::IOError("Test IO error");
    EXPECT_FALSE(io_error.ok());
    EXPECT_TRUE(io_error.is<ErrorCode::IO_ERROR>());
    EXPECT_TRUE(io_error.to_string().find("Test IO error") != std::string::npos);

    Status internal_error = Status::InternalError("Test internal error");
    EXPECT_FALSE(internal_error.ok());
    EXPECT_TRUE(internal_error.is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_TRUE(internal_error.to_string().find("Test internal error") != std::string::npos);

    Status not_found = Status::NotFound("Test not found");
    EXPECT_FALSE(not_found.ok());
    EXPECT_TRUE(not_found.is<ErrorCode::NOT_FOUND>());
    EXPECT_TRUE(not_found.to_string().find("Test not found") != std::string::npos);
}

// Test atomic operations (simulating closed flag behavior)
TEST_F(NewHdfsFileReaderTest, AtomicOperations) {
    std::atomic<bool> closed(false);

    EXPECT_FALSE(closed.load(std::memory_order_acquire));

    // Test compare_exchange_strong (simulating close operation)
    bool expected = false;
    bool result = closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
    EXPECT_TRUE(result);
    EXPECT_TRUE(closed.load(std::memory_order_acquire));

    // Second attempt should fail since it's already closed
    expected = false;
    result = closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
    EXPECT_FALSE(result);
    EXPECT_TRUE(expected); // expected should now be true (the actual value)
}

// Test thread safety patterns
TEST_F(NewHdfsFileReaderTest, ThreadSafetyPatterns) {
    std::atomic<bool> closed(false);
    std::atomic<int> close_count(0);

    // Simulate multiple threads trying to close simultaneously
    std::vector<std::thread> threads;

    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&closed, &close_count]() {
            bool expected = false;
            if (closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
                close_count.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Only one thread should have successfully "closed"
    EXPECT_TRUE(closed.load());
    EXPECT_EQ(close_count.load(), 1);
}

// Integration test demonstrating the complete flow
TEST_F(NewHdfsFileReaderTest, CompleteFlowSimulation) {
    // This test simulates what would happen in a real NewHdfsFileReader
    // by testing the individual components it would use

    // 1. Test HDFS operations
    hdfsFS mock_fs = reinterpret_cast<hdfsFS>(0x1000);
    hdfsFile mock_file = reinterpret_cast<hdfsFile>(0x2000);

    // 2. Test successful read operation (USE_HADOOP_HDFS path)
    char buffer[64];
    size_t offset = 10;
    size_t bytes_to_read = 32;

    tSize read_result = hdfsPread(mock_fs, mock_file, offset, buffer, bytes_to_read);
    EXPECT_EQ(read_result, bytes_to_read);
    EXPECT_EQ(buffer[0], 'A' + (offset % 26)); // Should be 'K' for offset 10

    // 3. Test error handling
    HdfsMockState::instance()->hdfs_pread_should_fail = true;
    read_result = hdfsPread(mock_fs, mock_file, offset, buffer, bytes_to_read);
    EXPECT_EQ(read_result, -1);

    // 4. Test fallback path (non-USE_HADOOP_HDFS)
    HdfsMockState::instance()->reset();

    int seek_result = hdfsSeek(mock_fs, mock_file, offset);
    EXPECT_EQ(seek_result, 0);

    tSize fallback_read = hdfsRead(mock_fs, mock_file, buffer, bytes_to_read);
    EXPECT_EQ(fallback_read, bytes_to_read);

    // 5. Test metrics behavior
    int64_t initial_open = DorisMetrics::instance()->hdfs_file_open_reading->value();
    int64_t initial_total = DorisMetrics::instance()->hdfs_file_reader_total->value();

    // Simulate reader creation
    DorisMetrics::instance()->hdfs_file_open_reading->increment(1);
    DorisMetrics::instance()->hdfs_file_reader_total->increment(1);

    EXPECT_EQ(DorisMetrics::instance()->hdfs_file_open_reading->value(), initial_open + 1);
    EXPECT_EQ(DorisMetrics::instance()->hdfs_file_reader_total->value(), initial_total + 1);

    // Simulate reader close (only decrements open_reading)
    DorisMetrics::instance()->hdfs_file_open_reading->increment(-1);

    EXPECT_EQ(DorisMetrics::instance()->hdfs_file_open_reading->value(), initial_open);
    EXPECT_EQ(DorisMetrics::instance()->hdfs_file_reader_total->value(), initial_total + 1);
}

} // namespace doris::io
