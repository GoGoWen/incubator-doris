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
#include <unordered_map>

#include "gen_cpp/PlanNodes_types.h"
#include "io/fs/file_handle_cache.h"
#include "io/fs/hdfs.h"
#include "io/fs/hdfs_file_system.h"

namespace doris::io {

// Forward declarations for test access
class HdfsFileSystemCache;
class HdfsFileHandleCache;

// Mock state for HDFS operations during testing
class HdfsMockState {
public:
    static HdfsMockState* instance() {
        static HdfsMockState s_instance;
        return &s_instance;
    }

    // Control flags for mocking different failure scenarios
    bool hdfs_connect_should_fail = false;
    bool hdfs_open_should_fail = false;
    bool hdfs_exists_should_fail = false;
    bool hdfs_get_path_info_should_fail = false;
    bool hdfs_list_directory_should_fail = false;
    bool hdfs_create_directory_should_fail = false;
    bool hdfs_delete_should_fail = false;
    bool hdfs_rename_should_fail = false;
    bool hdfs_read_should_fail = false;
    bool hdfs_pread_should_fail = false;
    bool hdfs_seek_should_fail = false;

    // Mock return values
    int mock_exists_return_value = 0; // 0 means exists, -1 means doesn't exist
    int mock_file_count = 0;

    // Mock filesystem handles
    std::unordered_map<std::string, hdfsFS> mock_fs_handles;

    void reset() {
        hdfs_connect_should_fail = false;
        hdfs_open_should_fail = false;
        hdfs_exists_should_fail = false;
        hdfs_get_path_info_should_fail = false;
        hdfs_list_directory_should_fail = false;
        hdfs_create_directory_should_fail = false;
        hdfs_delete_should_fail = false;
        hdfs_rename_should_fail = false;
        hdfs_read_should_fail = false;
        hdfs_pread_should_fail = false;
        hdfs_seek_should_fail = false;
        mock_exists_return_value = 0;
        mock_file_count = 0;
        mock_fs_handles.clear();
    }

    void cleanup() { reset(); }

private:
    HdfsMockState() = default;
};

} // namespace doris::io

// Mock HDFS C API functions
extern "C" {

// Mock hdfsBuilderConnect
hdfsFS mock_hdfsBuilderConnect(struct hdfsBuilder* bld);

// Mock hdfsDisconnect
int mock_hdfsDisconnect(hdfsFS fs);

// Mock hdfsExists
int mock_hdfsExists(hdfsFS fs, const char* path);

// Mock hdfsGetPathInfo
hdfsFileInfo* mock_hdfsGetPathInfo(hdfsFS fs, const char* path);

// Mock hdfsFreeFileInfo
void mock_hdfsFreeFileInfo(hdfsFileInfo* hdfsFileInfo, int numEntries);

// Mock hdfsListDirectory
hdfsFileInfo* mock_hdfsListDirectory(hdfsFS fs, const char* path, int* numEntries);

// Mock hdfsCreateDirectory
int mock_hdfsCreateDirectory(hdfsFS fs, const char* path);

// Mock hdfsDelete
int mock_hdfsDelete(hdfsFS fs, const char* path, int recursive);

// Mock hdfsRename
int mock_hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath);

// Mock hdfsOpenFile
hdfsFile mock_hdfsOpenFile(hdfsFS fs, const char* path, int flags, int bufferSize,
                           short replication, tSize blocksize);

// Mock hdfsCloseFile
int mock_hdfsCloseFile(hdfsFS fs, hdfsFile file);

// Mock hdfsRead
tSize mock_hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length);

// Mock hdfsWrite
tSize mock_hdfsWrite(hdfsFS fs, hdfsFile file, const void* buffer, tSize length);

// Mock hdfsFlush
int mock_hdfsFlush(hdfsFS fs, hdfsFile file);

// Mock hdfsSeek
int mock_hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos);

// Mock hdfsTell
tOffset mock_hdfsTell(hdfsFS fs, hdfsFile file);

// Mock hdfsPread
tSize mock_hdfsPread(hdfsFS fs, hdfsFile file, tOffset position, void* buffer, tSize length);

} // extern "C"

// Redefine HDFS function names to use mock implementations when testing
#ifdef HDFS_MOCK_ENABLED
#define hdfsBuilderConnect mock_hdfsBuilderConnect
#define hdfsDisconnect mock_hdfsDisconnect
#define hdfsExists mock_hdfsExists
#define hdfsGetPathInfo mock_hdfsGetPathInfo
#define hdfsFreeFileInfo mock_hdfsFreeFileInfo
#define hdfsListDirectory mock_hdfsListDirectory
#define hdfsCreateDirectory mock_hdfsCreateDirectory
#define hdfsDelete mock_hdfsDelete
#define hdfsRename mock_hdfsRename
#define hdfsOpenFile mock_hdfsOpenFile
#define hdfsCloseFile mock_hdfsCloseFile
#define hdfsRead mock_hdfsRead
#define hdfsWrite mock_hdfsWrite
#define hdfsFlush mock_hdfsFlush
#define hdfsSeek mock_hdfsSeek
#define hdfsTell mock_hdfsTell
#define hdfsPread mock_hdfsPread
#endif
