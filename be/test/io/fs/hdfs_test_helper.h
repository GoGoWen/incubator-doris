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

#pragma once

#include <memory>
#include <string>

#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/file_handle_cache.h"

namespace doris::io {

// Test helper class to access internal cache functionality
class HdfsTestHelper {
public:
    // Helper to access HdfsFileSystemCache functionality
    static Status get_filesystem_connection(const THdfsParams& hdfs_params, 
                                           const std::string& fs_name,
                                           std::shared_ptr<HdfsFileSystemHandle>* fs_handle);
    
    // Helper to access HdfsFileHandleCache functionality  
    static Status get_file_handle(std::shared_ptr<HdfsFileSystemHandle> fs_handle,
                                 const std::string& user,
                                 const std::string& fname,
                                 int64_t mtime,
                                 int64_t file_size,
                                 bool require_new_handle,
                                 FileHandleCache::Accessor* accessor,
                                 bool* cache_hit);
    
    // Helper to get HdfsFileHandleCache instance
    static void* get_file_handle_cache_instance();
    
    // Helper to check cache sizes (for testing cache consistency)
    static size_t get_filesystem_cache_size();
    static size_t get_filesystem_cache_keys_size();
};

} // namespace doris::io
