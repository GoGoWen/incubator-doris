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

#include "vec/exec/format/orc/vorc_reader.h"

#include <gtest/gtest.h>
#include <memory>
#include <vector>

#include "common/config.h"
#include "io/fs/file_reader.h"

namespace doris::vectorized {

class MockFileReader : public io::FileReader {
public:
    MockFileReader(size_t size) : _size(size) {
        _data.resize(size);
        for (size_t i = 0; i < size; ++i) {
            _data[i] = static_cast<char>(i % 256);
        }
    }

    Status close() override { return Status::OK(); }

    std::shared_ptr<io::FileSystem> fs() const override {
        static std::shared_ptr<io::FileSystem> fs;
        return fs;
    }

    const io::Path& path() const override {
        static io::Path path("test_file");
        return path;
    }

    size_t size() const override { return _size; }

    bool closed() const override { return false; }

    Status read_at(size_t offset, Slice result, size_t* bytes_read,
                  const io::IOContext* io_ctx = nullptr) {
        if (offset >= _size) {
            *bytes_read = 0;
            return Status::OK();
        }
        
        size_t to_read = std::min(result.size, _size - offset);
        memcpy(result.data, _data.data() + offset, to_read);
        *bytes_read = to_read;
        return Status::OK();
    }

    void collect_profile_before_close() {}

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                       const io::IOContext* io_ctx) override {
        return read_at(offset, result, bytes_read, io_ctx);
    }

private:
    size_t _size;
    std::vector<char> _data;
};

class ORCFileInputStreamTest : public ::testing::Test {
protected:
    void SetUp() override {
        _file_size = 10240;
        _mock_reader = std::make_shared<MockFileReader>(_file_size);
        
        _statistics = std::make_unique<OrcReader::Statistics>();
        _io_ctx = std::make_unique<io::IOContext>();
        
        _file_name = "test_orc_file";
        _input_stream = std::make_unique<ORCFileInputStream>(
            _file_name, _mock_reader, _statistics.get(), _io_ctx.get());
    }

    void TearDown() override {
        _input_stream.reset();
    }

    std::shared_ptr<MockFileReader> _mock_reader;
    std::unique_ptr<OrcReader::Statistics> _statistics;
    std::unique_ptr<io::IOContext> _io_ctx;
    std::string _file_name;
    std::unique_ptr<ORCFileInputStream> _input_stream;
    size_t _file_size;
};

TEST_F(ORCFileInputStreamTest, HitCacheBuffer_EmptyCache) {
    EXPECT_FALSE(_input_stream->hitCacheBuffer(0, 100));
    EXPECT_FALSE(_input_stream->hitCacheBuffer(100, 200));
}

TEST_F(ORCFileInputStreamTest, HitCacheBuffer_WithCache) {
    _input_stream->preadCache(orc::InputStream::CacheType::STRIPE, 1000, 512);
    
    EXPECT_TRUE(_input_stream->hitCacheBuffer(1000, 512));
    EXPECT_TRUE(_input_stream->hitCacheBuffer(1000, 256));
    EXPECT_TRUE(_input_stream->hitCacheBuffer(1200, 312));
    EXPECT_TRUE(_input_stream->hitCacheBuffer(1500, 12));
}

TEST_F(ORCFileInputStreamTest, HitCacheBuffer_OutsideCache) {
    _input_stream->preadCache(orc::InputStream::CacheType::STRIPE, 1000, 512);
    
    EXPECT_FALSE(_input_stream->hitCacheBuffer(500, 256));
    EXPECT_FALSE(_input_stream->hitCacheBuffer(1600, 100));
    EXPECT_FALSE(_input_stream->hitCacheBuffer(0, 512));
}

TEST_F(ORCFileInputStreamTest, HitCacheBuffer_PartialOverlap) {
    _input_stream->preadCache(orc::InputStream::CacheType::STRIPE, 1000, 512);
    
    EXPECT_FALSE(_input_stream->hitCacheBuffer(500, 600));
    EXPECT_FALSE(_input_stream->hitCacheBuffer(1200, 400));
}

TEST_F(ORCFileInputStreamTest, HitCacheBuffer_ExactBoundaries) {
    _input_stream->preadCache(orc::InputStream::CacheType::STRIPE, 1000, 512);
    
    EXPECT_TRUE(_input_stream->hitCacheBuffer(1000, 1));
    EXPECT_TRUE(_input_stream->hitCacheBuffer(1511, 1));
    EXPECT_FALSE(_input_stream->hitCacheBuffer(999, 1));
    EXPECT_FALSE(_input_stream->hitCacheBuffer(1512, 1));
}

TEST_F(ORCFileInputStreamTest, PreadCache_BasicFunctionality) {
    _input_stream->preadCache(orc::InputStream::CacheType::STRIPE, 1000, 512);
    
    EXPECT_TRUE(_input_stream->hitCacheBuffer(1000, 512));
    EXPECT_TRUE(_input_stream->hitCacheBuffer(1200, 100));
}

TEST_F(ORCFileInputStreamTest, PreadCache_TooLarge) {
    size_t original_cache_size = config::orc_file_cache_buffer_size;
    config::orc_file_cache_buffer_size = 1000;
    
    _input_stream->preadCache(orc::InputStream::CacheType::STRIPE, 1000, 2000);
    
    EXPECT_FALSE(_input_stream->hitCacheBuffer(1000, 100));
    
    config::orc_file_cache_buffer_size = original_cache_size;
}

TEST_F(ORCFileInputStreamTest, PreadCache_AlreadyCached) {
    _input_stream->preadCache(orc::InputStream::CacheType::STRIPE, 1000, 512);
    EXPECT_TRUE(_input_stream->hitCacheBuffer(1000, 512));
    
    _input_stream->preadCache(orc::InputStream::CacheType::STRIPE, 1000, 512);
    EXPECT_TRUE(_input_stream->hitCacheBuffer(1000, 512));
}


TEST_F(ORCFileInputStreamTest, Read_UsesCache) {
    _input_stream->preadCache(orc::InputStream::CacheType::STRIPE, 1000, 512);
    
    std::vector<char> buffer(256);
    _input_stream->read(buffer.data(), 256, 1200);
    
    for (size_t i = 0; i < 256; ++i) {
        EXPECT_EQ(buffer[i], static_cast<char>((1200 + i) % 256));
    }
}

TEST_F(ORCFileInputStreamTest, Read_FallsBackToPread) {
    std::vector<char> buffer(256);
    _input_stream->read(buffer.data(), 256, 2000);
    
    for (size_t i = 0; i < 256; ++i) {
        EXPECT_EQ(buffer[i], static_cast<char>((2000 + i) % 256));
    }
}

TEST_F(ORCFileInputStreamTest, PreadCache_MultipleRegions) {
    _input_stream->preadCache(orc::InputStream::CacheType::STRIPE, 1000, 512);
    EXPECT_TRUE(_input_stream->hitCacheBuffer(1000, 512));
    
    _input_stream->preadCache(orc::InputStream::CacheType::STRIPE, 2000, 256);
    EXPECT_TRUE(_input_stream->hitCacheBuffer(2000, 256));
    EXPECT_FALSE(_input_stream->hitCacheBuffer(1000, 512));
}

TEST_F(ORCFileInputStreamTest, Read_CacheIntegration) {
    std::vector<char> buffer1(256);
    std::vector<char> buffer2(256);
    
    _input_stream->preadCache(orc::InputStream::CacheType::STRIPE, 1000, 1024);
    
    _input_stream->read(buffer1.data(), 256, 1000);
    _input_stream->read(buffer2.data(), 256, 1256);
    
    for (size_t i = 0; i < 256; ++i) {
        EXPECT_EQ(buffer1[i], static_cast<char>((1000 + i) % 256));
        EXPECT_EQ(buffer2[i], static_cast<char>((1256 + i) % 256));
    }
}

} // namespace doris::vectorized
