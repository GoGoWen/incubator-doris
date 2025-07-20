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

#include "hdfs_test_mocks.h"

#include <cstdlib>
#include <cstring>

namespace doris::io {

// Mock implementations of HDFS C API functions
extern "C" {

hdfsFS mock_hdfsBuilderConnect(struct hdfsBuilder* bld) {
    if (HdfsMockState::instance()->hdfs_connect_should_fail) {
        return nullptr;
    }

    // Return a mock filesystem handle (just a non-null pointer)
    static int mock_fs_counter = 1;
    hdfsFS mock_fs = reinterpret_cast<hdfsFS>(&mock_fs_counter);
    mock_fs_counter++;

    return mock_fs;
}

int mock_hdfsDisconnect(hdfsFS fs) {
    if (fs == nullptr) {
        return -1;
    }
    return 0; // Success
}

int mock_hdfsExists(hdfsFS fs, const char* path) {
    if (HdfsMockState::instance()->hdfs_exists_should_fail) {
        return -1; // Error
    }
    return HdfsMockState::instance()->mock_exists_return_value;
}

hdfsFileInfo* mock_hdfsGetPathInfo(hdfsFS fs, const char* path) {
    if (HdfsMockState::instance()->hdfs_get_path_info_should_fail) {
        return nullptr;
    }

    // Allocate and return a mock hdfsFileInfo
    hdfsFileInfo* info = static_cast<hdfsFileInfo*>(malloc(sizeof(hdfsFileInfo)));
    if (info) {
        memset(info, 0, sizeof(hdfsFileInfo));
        info->mName = strdup(path);
        info->mSize = 1024; // Mock file size
        info->mKind = kObjectKindFile;
        info->mLastMod = 1234567890; // Mock timestamp
        info->mReplication = 3;
        info->mBlockSize = 134217728; // 128MB
        info->mOwner = strdup("test_user");
        info->mGroup = strdup("test_group");
        info->mPermissions = 0644;
    }
    return info;
}

void mock_hdfsFreeFileInfo(hdfsFileInfo* hdfsFileInfo, int numEntries) {
    if (hdfsFileInfo) {
        for (int i = 0; i < numEntries; i++) {
            free(hdfsFileInfo[i].mName);
            free(hdfsFileInfo[i].mOwner);
            free(hdfsFileInfo[i].mGroup);
        }
        free(hdfsFileInfo);
    }
}

hdfsFileInfo* mock_hdfsListDirectory(hdfsFS fs, const char* path, int* numEntries) {
    if (HdfsMockState::instance()->hdfs_list_directory_should_fail) {
        *numEntries = 0;
        return nullptr;
    }

    int count = HdfsMockState::instance()->mock_file_count;
    if (count <= 0) {
        count = 2; // Default to 2 mock files
    }

    hdfsFileInfo* infos = static_cast<hdfsFileInfo*>(malloc(sizeof(hdfsFileInfo) * count));
    if (infos) {
        for (int i = 0; i < count; i++) {
            memset(&infos[i], 0, sizeof(hdfsFileInfo));

            std::string filename = std::string(path) + "/file" + std::to_string(i) + ".txt";
            infos[i].mName = strdup(filename.c_str());
            infos[i].mSize = 1024 * (i + 1); // Different sizes
            infos[i].mKind = kObjectKindFile;
            infos[i].mLastMod = 1234567890 + i;
            infos[i].mReplication = 3;
            infos[i].mBlockSize = 134217728;
            infos[i].mOwner = strdup("test_user");
            infos[i].mGroup = strdup("test_group");
            infos[i].mPermissions = 0644;
        }
    }

    *numEntries = count;
    return infos;
}

int mock_hdfsCreateDirectory(hdfsFS fs, const char* path) {
    if (HdfsMockState::instance()->hdfs_create_directory_should_fail) {
        return -1;
    }
    return 0; // Success
}

int mock_hdfsDelete(hdfsFS fs, const char* path, int recursive) {
    if (HdfsMockState::instance()->hdfs_delete_should_fail) {
        return -1;
    }
    return 0; // Success
}

int mock_hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath) {
    if (HdfsMockState::instance()->hdfs_rename_should_fail) {
        return -1;
    }
    return 0; // Success
}

hdfsFile mock_hdfsOpenFile(hdfsFS fs, const char* path, int flags, int bufferSize,
                           short replication, tSize blocksize) {
    if (HdfsMockState::instance()->hdfs_open_should_fail) {
        return nullptr;
    }

    // Return a mock file handle (just a non-null pointer)
    static int mock_file_counter = 1000;
    hdfsFile mock_file = reinterpret_cast<hdfsFile>(&mock_file_counter);
    mock_file_counter++;

    return mock_file;
}

int mock_hdfsCloseFile(hdfsFS fs, hdfsFile file) {
    if (file == nullptr) {
        return -1;
    }
    return 0; // Success
}

tSize mock_hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length) {
    if (file == nullptr || buffer == nullptr) {
        return -1;
    }

    // Fill buffer with mock data
    memset(buffer, 'A', length);
    return length;
}

tSize mock_hdfsWrite(hdfsFS fs, hdfsFile file, const void* buffer, tSize length) {
    if (file == nullptr || buffer == nullptr) {
        return -1;
    }
    return length; // Pretend we wrote all data
}

int mock_hdfsFlush(hdfsFS fs, hdfsFile file) {
    if (file == nullptr) {
        return -1;
    }
    return 0; // Success
}

int mock_hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos) {
    if (file == nullptr) {
        return -1;
    }
    return 0; // Success
}

tOffset mock_hdfsTell(hdfsFS fs, hdfsFile file) {
    if (file == nullptr) {
        return -1;
    }
    return 0; // Mock position
}

} // extern "C"

} // namespace doris::io
