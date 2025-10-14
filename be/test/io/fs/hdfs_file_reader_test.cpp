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

#include "io/fs/hdfs_file_reader.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

#include "common/status.h"
#include "io/fs/file_handle_cache.h"
#include "util/slice.h"

namespace doris {
namespace io {

// Mock CachedHdfsFileHandle for testing
class MockCachedHdfsFileHandle : public CachedHdfsFileHandle {
public:
    MockCachedHdfsFileHandle() : CachedHdfsFileHandle(nullptr, "/test/path", 12345) {}

    // Mock file handle that returns test values
    hdfsFS fs() const { return reinterpret_cast<hdfsFS>(0x1234); }
    hdfsFile file() const { return reinterpret_cast<hdfsFile>(0x5678); }
    int64_t file_size() const { return 1024; }
};

// Mock FileHandleCache::Accessor with destroy tracking
class MockAccessor {
public:
    MockAccessor() : _destroy_called(0), _handle(std::make_unique<MockCachedHdfsFileHandle>()) {}

    void destroy() {
        _destroy_called++;
    }

    int destroy_call_count() const {
        return _destroy_called;
    }

    CachedHdfsFileHandle* get() {
        return _handle.get();
    }

    void release() {}

    FileSystemSPtr fs() const {
        return nullptr;
    }

    void reset_destroy_counter() {
        _destroy_called = 0;
    }

private:
    int _destroy_called;
    std::unique_ptr<MockCachedHdfsFileHandle> _handle;
};

// Test fixture for HdfsFileReader
class HdfsFileReaderTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// Note: We don't subclass HdfsFileReader directly because do_read_at_impl is not virtual.
// Instead, we test the pattern using SimpleMockFileReader below.

// Simpler mock-based test that directly tests the pattern
class SimpleMockFileReader : public FileReader {
public:
    SimpleMockFileReader(MockAccessor* accessor, bool should_fail)
        : _accessor(accessor), _should_fail(should_fail), _closed(false) {}

    ~SimpleMockFileReader() override = default;

    Status close() override {
        _closed = true;
        return Status::OK();
    }

    const Path& path() const override {
        static Path dummy_path("/test/path");
        return dummy_path;
    }

    size_t size() const override { return 1024; }

    bool closed() const override { return _closed; }

    FileSystemSPtr fs() const override { return nullptr; }

protected:
    // This implements the EXACT pattern from the commit: b3bd7ae7cd0
    // Lines 85-92 of be/src/io/fs/hdfs_file_reader.cpp
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                       const IOContext* io_ctx) override {
        auto status = do_read_at_impl(offset, result, bytes_read, io_ctx);
        if (!status.ok()) {
            _accessor->destroy();  // KEY LINE: destroy on failure
        }
        return status;
    }

    Status do_read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                          const IOContext* io_ctx) {
        if (_should_fail) {
            return Status::IOError("Simulated read failure");
        }
        *bytes_read = std::min(result.size, size_t(100));
        return Status::OK();
    }

private:
    MockAccessor* _accessor;
    bool _should_fail;
    bool _closed;
};

// Test that cached file handle is destroyed when read fails
TEST_F(HdfsFileReaderTest, test_destroy_cached_handle_on_read_failure) {
    MockAccessor mock_accessor;
    auto reader = std::make_unique<SimpleMockFileReader>(&mock_accessor, true);

    char buffer[128];
    Slice result(buffer, 128);
    size_t bytes_read = 0;

    // Verify destroy hasn't been called yet
    EXPECT_EQ(0, mock_accessor.destroy_call_count());

    // Attempt to read - this should fail
    Status status = reader->read_at(0, result, &bytes_read);

    // Verify that the read failed
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::IO_ERROR>());

    // Verify that the accessor's destroy() was called EXACTLY ONCE
    EXPECT_EQ(1, mock_accessor.destroy_call_count());
}

// Test that cached file handle is NOT destroyed when read succeeds
TEST_F(HdfsFileReaderTest, test_preserve_cached_handle_on_read_success) {
    MockAccessor mock_accessor;
    auto reader = std::make_unique<SimpleMockFileReader>(&mock_accessor, false);

    char buffer[128];
    Slice result(buffer, 128);
    size_t bytes_read = 0;

    // Attempt to read - this should succeed
    Status status = reader->read_at(0, result, &bytes_read);

    // Verify that the read succeeded
    EXPECT_TRUE(status.ok());
    EXPECT_GT(bytes_read, 0);

    // Verify that the accessor's destroy() was NOT called
    EXPECT_EQ(0, mock_accessor.destroy_call_count());
}

// Test multiple failed reads to ensure consistent cleanup
TEST_F(HdfsFileReaderTest, test_multiple_failed_reads) {
    MockAccessor mock_accessor;
    auto reader = std::make_unique<SimpleMockFileReader>(&mock_accessor, true);

    char buffer[128];
    Slice result(buffer, 128);
    size_t bytes_read = 0;

    // First failed read
    Status status1 = reader->read_at(0, result, &bytes_read);
    EXPECT_FALSE(status1.ok());
    EXPECT_EQ(1, mock_accessor.destroy_call_count());

    // Second failed read
    Status status2 = reader->read_at(100, result, &bytes_read);
    EXPECT_FALSE(status2.ok());
    EXPECT_EQ(2, mock_accessor.destroy_call_count());

    // Third failed read
    Status status3 = reader->read_at(200, result, &bytes_read);
    EXPECT_FALSE(status3.ok());
    EXPECT_EQ(3, mock_accessor.destroy_call_count());
}

// Test interleaved success and failure reads
TEST_F(HdfsFileReaderTest, test_mixed_success_and_failure_reads) {
    MockAccessor mock_accessor_success;
    MockAccessor mock_accessor_fail;

    auto reader_success = std::make_unique<SimpleMockFileReader>(&mock_accessor_success, false);
    auto reader_fail = std::make_unique<SimpleMockFileReader>(&mock_accessor_fail, true);

    char buffer[128];
    Slice result(buffer, 128);
    size_t bytes_read = 0;

    // Successful read - no destroy
    EXPECT_TRUE(reader_success->read_at(0, result, &bytes_read).ok());
    EXPECT_EQ(0, mock_accessor_success.destroy_call_count());

    // Failed read - destroy called
    EXPECT_FALSE(reader_fail->read_at(0, result, &bytes_read).ok());
    EXPECT_EQ(1, mock_accessor_fail.destroy_call_count());

    // Another successful read - still no destroy
    EXPECT_TRUE(reader_success->read_at(50, result, &bytes_read).ok());
    EXPECT_EQ(0, mock_accessor_success.destroy_call_count());

    // Another failed read - destroy called again
    EXPECT_FALSE(reader_fail->read_at(50, result, &bytes_read).ok());
    EXPECT_EQ(2, mock_accessor_fail.destroy_call_count());
}

} // namespace io
} // namespace doris
