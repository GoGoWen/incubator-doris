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

#include "hdfs_test_helper.h"

// We need to include the implementation to access the internal classes
// This is a test-specific approach to access private implementation details
#define HDFS_MOCK_ENABLED
#include "hdfs_test_mocks.h"

// Include the source file to get access to internal classes
// This is only for testing purposes
namespace doris::io {

// Forward declare the internal classes that are defined in hdfs_file_system.cpp
class HdfsFileSystemCache {
public:
    static HdfsFileSystemCache* instance();
    Status get_connection(const THdfsParams& hdfs_params, const std::string& fs_name,
                          std::shared_ptr<HdfsFileSystemHandle>* fs_handle);
    
    // For testing access to private members
    size_t cache_size() const;
    size_t cache_keys_size() const;
};

class HdfsFileHandleCache {
public:
    static HdfsFileHandleCache* instance();
    FileHandleCache& cache();
};

// Implementations of test helper functions
Status HdfsTestHelper::get_filesystem_connection(const THdfsParams& hdfs_params, 
                                                const std::string& fs_name,
                                                std::shared_ptr<HdfsFileSystemHandle>* fs_handle) {
    return HdfsFileSystemCache::instance()->get_connection(hdfs_params, fs_name, fs_handle);
}

Status HdfsTestHelper::get_file_handle(std::shared_ptr<HdfsFileSystemHandle> fs_handle,
                                      const std::string& user,
                                      const std::string& fname,
                                      int64_t mtime,
                                      int64_t file_size,
                                      bool require_new_handle,
                                      FileHandleCache::Accessor* accessor,
                                      bool* cache_hit) {
    return HdfsFileHandleCache::instance()->cache().get_file_handle(
        fs_handle, user, fname, mtime, file_size, require_new_handle, accessor, cache_hit);
}

void* HdfsTestHelper::get_file_handle_cache_instance() {
    return HdfsFileHandleCache::instance();
}

size_t HdfsTestHelper::get_filesystem_cache_size() {
    // This would need to be implemented with friend access or reflection
    // For now, return 0 as placeholder
    return 0;
}

size_t HdfsTestHelper::get_filesystem_cache_keys_size() {
    // This would need to be implemented with friend access or reflection
    // For now, return 0 as placeholder
    return 0;
}

} // namespace doris::io
