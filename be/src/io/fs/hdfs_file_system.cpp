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

#include "io/fs/hdfs_file_system.h"

#include <errno.h>
#include <fcntl.h>
#include <gen_cpp/PlanNodes_types.h>
#include <limits.h>
#include <stddef.h>

#include <algorithm>
#include <condition_variable>
#include <filesystem>
#include <map>
#include <mutex>
#include <ostream>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/config.h"
#include "gutil/hash/hash.h"
#include "gutil/integral_types.h"
#include "io/fs/err_utils.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/hdfs_file_reader.h"
#include "io/fs/hdfs_file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/new_hdfs_file_reader.h"
#include "io/hdfs_builder.h"
#include "util/hdfs_util.h"
#include "util/obj_lru_cache.h"
#include "util/slice.h"

namespace doris {
namespace io {

#ifndef CHECK_HDFS_HANDLE
#define CHECK_HDFS_HANDLE(handle)                         \
    if (!handle) {                                        \
        return Status::IOError("init Hdfs handle error"); \
    }
#endif

class RandomGenerator {
private:
    std::random_device _rd;
    std::mt19937 _gen;

public:
    RandomGenerator() : _gen(_rd()) {}
    uint32_t uniform(uint32_t max_value) {
        std::uniform_int_distribution<uint32_t> dis(0, max_value - 1);
        return dis(_gen);
    }
};
// Cache for HdfsFileSystemHandle
class HdfsFileSystemCache {
public:
    static HdfsFileSystemCache* instance() {
        static HdfsFileSystemCache s_instance;
        return &s_instance;
    }

    HdfsFileSystemCache(const HdfsFileSystemCache&) = delete;
    const HdfsFileSystemCache& operator=(const HdfsFileSystemCache&) = delete;

    // This function is thread-safe
    Status get_connection(const THdfsParams& hdfs_params, const std::string& fs_name,
                          std::shared_ptr<HdfsFileSystemHandle>* fs_handle);

private:
    std::mutex _lock;
    std::condition_variable _creation_cv;
    std::unordered_set<std::string> _creating_fs;

    std::unordered_map<std::string, std::shared_ptr<HdfsFileSystemHandle>> _cache;
    std::vector<std::string> _cache_keys;
    RandomGenerator _rand;

    HdfsFileSystemCache() = default;

    std::string _hdfs_cache_key(const THdfsParams& hdfs_params, const std::string& fs_name);
    Status _create_fs(const THdfsParams& hdfs_params, const std::string& fs_name, hdfsFS* fs);
};

class HdfsFileHandleCache {
public:
    static HdfsFileHandleCache* instance() {
        static HdfsFileHandleCache s_instance;
        return &s_instance;
    }

    HdfsFileHandleCache(const HdfsFileHandleCache&) = delete;
    const HdfsFileHandleCache& operator=(const HdfsFileHandleCache&) = delete;

    FileHandleCache& cache() { return _cache; }

    // try get hdfs file from cache, if not exists, will open a new file, insert it into cache
    // and return the file cache handle.
    Status get_file(const std::shared_ptr<HdfsFileSystem>& fs, const Path& file, int64_t mtime,
                    int64_t file_size, FileHandleCache::Accessor* accessor,
                    const hdfsAuditContext* audit_context);

private:
    FileHandleCache _cache;
    HdfsFileHandleCache()
            : _cache(config::max_hdfs_file_handle_cache_num,
                     config::num_partitions_for_hdfs_file_handle_cache,
                     config::max_hdfs_file_handle_cache_time_sec) {};
};

Status HdfsFileHandleCache::get_file(const std::shared_ptr<HdfsFileSystem>& fs, const Path& file,
                                     int64_t mtime, int64_t file_size,
                                     FileHandleCache::Accessor* accessor,
                                     const hdfsAuditContext* audit_context) {
    bool cache_hit;
    std::string fname = file.string();
    RETURN_IF_ERROR(HdfsFileHandleCache::instance()->cache().get_file_handle(
            fs->_fs_handle, fs->_hdfs_params.user, fname, mtime, file_size, false, accessor,
            &cache_hit, audit_context));
    accessor->set_fs(std::static_pointer_cast<FileSystem>(fs));

    return Status::OK();
}

Status HdfsFileSystem::create(const THdfsParams& hdfs_params, std::string id,
                              const std::string& fs_name, RuntimeProfile* profile,
                              std::shared_ptr<HdfsFileSystem>* fs) {
#ifdef USE_HADOOP_HDFS
    if (!config::enable_java_support) {
        return Status::InternalError(
                "hdfs file system is not enabled, you can change be config enable_java_support to "
                "true.");
    }
#endif
    (*fs).reset(new HdfsFileSystem(hdfs_params, std::move(id), fs_name, profile));
    return (*fs)->connect();
}

HdfsFileSystem::HdfsFileSystem(const THdfsParams& hdfs_params, std::string id,
                               const std::string& fs_name, RuntimeProfile* profile)
        : RemoteFileSystem("", std::move(id), FileSystemType::HDFS),
          _hdfs_params(hdfs_params),
          _fs_handle(nullptr),
          _profile(profile) {
    if (fs_name.empty() && _hdfs_params.__isset.fs_name) {
        _fs_name = _hdfs_params.fs_name;
    } else {
        _fs_name = fs_name;
    }

    for (const auto& conf : _hdfs_params.hdfs_conf) {
        if (conf.key == "BEE_BUSINESSID") {
            _audit_context_strings["businessId"] = conf.value;
        } else if (conf.key == "BEE_USER") {
            _audit_context_strings["erp"] = conf.value;
        } else if (conf.key == "BEE_SOURCE") {
            _audit_context_strings["source"] = conf.value;
        }
    }

    if (_audit_context_strings.contains("businessId")) {
        _audit_context.businessId = _audit_context_strings["businessId"].c_str();
    }
    if (_audit_context_strings.contains("erp")) {
        _audit_context.erp = _audit_context_strings["erp"].c_str();
    }
    if (_audit_context_strings.contains("source")) {
        _audit_context.source = _audit_context_strings["source"].c_str();
    }
}

HdfsFileSystem::~HdfsFileSystem() = default;

Status HdfsFileSystem::connect_impl() {
    RETURN_IF_ERROR(
            HdfsFileSystemCache::instance()->get_connection(_hdfs_params, _fs_name, &_fs_handle));
    if (!_fs_handle) {
        return Status::IOError("failed to init Hdfs handle with, please check hdfs params.");
    }
    return Status::OK();
}

Status HdfsFileSystem::create_file_impl(const Path& file, FileWriterPtr* writer,
                                        const FileWriterOptions* opts) {
    *writer = std::make_unique<HdfsFileWriter>(file, getSPtr(), opts);
    return Status::OK();
}

Status HdfsFileSystem::open_file_internal(const Path& file, FileReaderSPtr* reader,
                                          const FileReaderOptions& opts) {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(file, _fs_name);

    if (config::enable_hdfs_file_handle_cache) {
        FileHandleCache::Accessor accessor;
        RETURN_IF_ERROR(HdfsFileHandleCache::instance()->get_file(
                std::static_pointer_cast<HdfsFileSystem>(shared_from_this()), real_path, opts.mtime,
                opts.file_size, &accessor, &_audit_context));
        *reader = std::make_shared<HdfsFileReader>(file, _fs_name, std::move(accessor), _profile);
    } else {
        std::unique_ptr<ExclusiveHdfsFileHandle> hdfs_file_handle =
                std::make_unique<ExclusiveHdfsFileHandle>(
                        std::static_pointer_cast<HdfsFileSystem>(shared_from_this())->_fs_handle,
                        real_path.string(), opts.mtime);
        RETURN_IF_ERROR(hdfs_file_handle->init(opts.file_size, &_audit_context));
        *reader = std::make_shared<NewHdfsFileReader>(
                file, _fs_name, std::static_pointer_cast<HdfsFileSystem>(shared_from_this()),
                std::move(hdfs_file_handle), _profile);
    }

    return Status::OK();
}

Status HdfsFileSystem::create_directory_impl(const Path& dir, bool failed_if_exists) {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(dir, _fs_name);
    int res = hdfsCreateDirectory(_fs_handle->hdfs_fs, real_path.string().c_str());
    if (res == -1) {
        return Status::IOError("failed to create directory {}: {}", dir.native(), hdfs_error());
    }
    return Status::OK();
}

Status HdfsFileSystem::delete_file_impl(const Path& file) {
    return delete_internal(file, 0);
}

Status HdfsFileSystem::delete_directory_impl(const Path& dir) {
    return delete_internal(dir, 1);
}

Status HdfsFileSystem::batch_delete_impl(const std::vector<Path>& files) {
    for (auto& file : files) {
        RETURN_IF_ERROR(delete_file_impl(file));
    }
    return Status::OK();
}

Status HdfsFileSystem::delete_internal(const Path& path, int is_recursive) {
    bool exists = true;
    RETURN_IF_ERROR(exists_impl(path, &exists));
    if (!exists) {
        return Status::OK();
    }
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(path, _fs_name);
    int res = hdfsDelete(_fs_handle->hdfs_fs, real_path.string().c_str(), is_recursive);
    if (res == -1) {
        return Status::IOError("failed to delete directory {}: {}", path.native(), hdfs_error());
    }
    return Status::OK();
}

Status HdfsFileSystem::exists_impl(const Path& path, bool* res) const {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(path, _fs_name);

    int is_exists = hdfsExistsWithAuditContext(_fs_handle->hdfs_fs, real_path.string().c_str(),
                                               &_audit_context);
#ifdef USE_HADOOP_HDFS
    // when calling hdfsExists() and return non-zero code,
    // if errno is ENOENT, which means the file does not exist.
    // if errno is not ENOENT, which means it encounter other error, should return.
    // NOTE: not for libhdfs3 since it only runs on MaxOS, don't have to support it.
    //
    // See details:
    //  https://github.com/apache/hadoop/blob/5cda162a804fb0cfc2a5ac0058ab407662c5fb00/
    //  hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/hdfs.c#L1923-L1924
    if (is_exists != 0 && errno != ENOENT) {
        return Status::IOError("failed to check path existence {}: {}", path.native(),
                               hdfs_error());
    }
#endif
    *res = (is_exists == 0);
    return Status::OK();
}

Status HdfsFileSystem::file_size_impl(const Path& path, int64_t* file_size) const {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(path, _fs_name);

    hdfsFileInfo* file_info = hdfsGetPathInfoWithAuditContext(
            _fs_handle->hdfs_fs, real_path.string().c_str(), &_audit_context);
    if (file_info == nullptr) {
        return Status::IOError("failed to get file size of {}: {}", path.native(), hdfs_error());
    }
    *file_size = file_info->mSize;
    hdfsFreeFileInfo(file_info, 1);
    return Status::OK();
}

Status HdfsFileSystem::list_impl(const Path& path, bool only_file, std::vector<FileInfo>* files,
                                 bool* exists) {
    RETURN_IF_ERROR(exists_impl(path, exists));
    if (!(*exists)) {
        return Status::OK();
    }

    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(path, _fs_name);
    int numEntries = 0;
    hdfsFileInfo* hdfs_file_info = hdfsListDirectoryWithAuditContext(
            _fs_handle->hdfs_fs, real_path.c_str(), &numEntries, &_audit_context);
    if (hdfs_file_info == nullptr) {
        return Status::IOError("failed to list files/directors {}: {}", path.native(),
                               hdfs_error());
    }
    for (int idx = 0; idx < numEntries; ++idx) {
        auto& file = hdfs_file_info[idx];
        if (only_file && file.mKind == kObjectKindDirectory) {
            continue;
        }
        auto& file_info = files->emplace_back();
        std::string_view fname(file.mName);
        fname.remove_prefix(fname.rfind('/') + 1);
        file_info.file_name = fname;
        file_info.file_size = file.mSize;
        file_info.is_file = (file.mKind != kObjectKindDirectory);
    }
    hdfsFreeFileInfo(hdfs_file_info, numEntries);
    return Status::OK();
}

Status HdfsFileSystem::rename_impl(const Path& orig_name, const Path& new_name) {
    Path normal_orig_name = convert_path(orig_name, _fs_name);
    Path normal_new_name = convert_path(new_name, _fs_name);
    int ret = hdfsRename(_fs_handle->hdfs_fs, normal_orig_name.c_str(), normal_new_name.c_str());
    if (ret == 0) {
        LOG(INFO) << "finished to rename file. orig: " << normal_orig_name
                  << ", new: " << normal_new_name;
        return Status::OK();
    } else {
        return Status::IOError("fail to rename from {} to {}: {}", normal_orig_name.native(),
                               normal_new_name.native(), hdfs_error());
    }
    return Status::OK();
}

Status HdfsFileSystem::upload_impl(const Path& local_file, const Path& remote_file) {
    // 1. open local file for read
    FileSystemSPtr local_fs = global_local_filesystem();
    FileReaderSPtr local_reader = nullptr;
    RETURN_IF_ERROR(local_fs->open_file(local_file, &local_reader));
    int64_t file_len = local_reader->size();
    if (file_len == -1) {
        return Status::IOError("failed to get size of file: {}", local_file.string());
    }

    // 2. open remote file for write
    FileWriterPtr hdfs_writer = nullptr;
    RETURN_IF_ERROR(create_file_impl(remote_file, &hdfs_writer, nullptr));

    constexpr size_t buf_sz = 1024 * 1024;
    char read_buf[buf_sz];
    size_t left_len = file_len;
    size_t read_offset = 0;
    size_t bytes_read = 0;
    while (left_len > 0) {
        size_t read_len = left_len > buf_sz ? buf_sz : left_len;
        RETURN_IF_ERROR(local_reader->read_at(read_offset, {read_buf, read_len}, &bytes_read));
        RETURN_IF_ERROR(hdfs_writer->append({read_buf, read_len}));

        read_offset += read_len;
        left_len -= read_len;
    }

    LOG(INFO) << "finished to write file: " << local_file << ", length: " << file_len;
    return Status::OK();
}

Status HdfsFileSystem::batch_upload_impl(const std::vector<Path>& local_files,
                                         const std::vector<Path>& remote_files) {
    DCHECK(local_files.size() == remote_files.size());
    for (int i = 0; i < local_files.size(); ++i) {
        RETURN_IF_ERROR(upload_impl(local_files[i], remote_files[i]));
    }
    return Status::OK();
}

Status HdfsFileSystem::download_impl(const Path& remote_file, const Path& local_file) {
    // 1. open remote file for read
    FileReaderSPtr hdfs_reader = nullptr;
    RETURN_IF_ERROR(open_file_internal(remote_file, &hdfs_reader, FileReaderOptions::DEFAULT));

    // 2. remove the existing local file if exist
    if (std::filesystem::remove(local_file)) {
        LOG(INFO) << "remove the previously exist local file: " << local_file;
    }

    // 3. open local file for write
    FileSystemSPtr local_fs = global_local_filesystem();
    FileWriterPtr local_writer = nullptr;
    RETURN_IF_ERROR(local_fs->create_file(local_file, &local_writer));

    // 4. read remote and write to local
    LOG(INFO) << "read remote file: " << remote_file << " to local: " << local_file;
    constexpr size_t buf_sz = 1024 * 1024;
    std::unique_ptr<char[]> read_buf(new char[buf_sz]);
    size_t cur_offset = 0;
    while (true) {
        size_t read_len = 0;
        Slice file_slice(read_buf.get(), buf_sz);
        RETURN_IF_ERROR(hdfs_reader->read_at(cur_offset, file_slice, &read_len));
        cur_offset += read_len;
        if (read_len == 0) {
            break;
        }

        RETURN_IF_ERROR(local_writer->append({read_buf.get(), read_len}));
    }
    return local_writer->close();
}

Status HdfsFileSystemCache::_create_fs(const THdfsParams& hdfs_params, const std::string& fs_name,
                                       hdfsFS* fs) {
    HDFSCommonBuilder builder;
    RETURN_IF_ERROR(create_hdfs_builder(hdfs_params, fs_name, &builder));
    hdfsFS hdfs_fs = hdfsBuilderConnect(builder.get());
    if (hdfs_fs == nullptr) {
        return Status::IOError("faield to connect to hdfs {}: {}", fs_name, hdfs_error());
    }
    *fs = hdfs_fs;
    return Status::OK();
}

Status HdfsFileSystemCache::get_connection(const THdfsParams& hdfs_params,
                                           const std::string& fs_name,
                                           std::shared_ptr<HdfsFileSystemHandle>* fs_handle) {
    std::string cache_key = _hdfs_cache_key(hdfs_params, fs_name);
    std::unique_lock<std::mutex> lock(_lock);

    while (true) {
        auto it = _cache.find(cache_key);
        if (it != _cache.end()) {
            std::shared_ptr<HdfsFileSystemHandle> handle = it->second;
            if (!handle->invalid()) {
                *fs_handle = handle;
                return Status::OK();
            }
            // fs handle is invalid, erase it.
            _cache.erase(it);
            auto key_it = std::find(_cache_keys.begin(), _cache_keys.end(), cache_key);
            if (key_it != _cache_keys.end()) {
                _cache_keys.erase(key_it);
            }
        }

        if (_creating_fs.find(cache_key) != _creating_fs.end()) {
            _creation_cv.wait(lock);
            continue;
        }

        _creating_fs.insert(cache_key);
        break;
    }

    lock.unlock();

    hdfsFS hdfs_fs = nullptr;
    Status create_status = _create_fs(hdfs_params, fs_name, &hdfs_fs);

    lock.lock();

    if (create_status.ok()) {
        const uint32_t max_cache_size = config::max_hdfs_file_system_cache_num;
        auto handle = std::make_shared<HdfsFileSystemHandle>(hdfs_fs, true);
        *fs_handle = handle;

        if (_cache_keys.size() >= max_cache_size) {
            uint32_t idx = _rand.uniform(max_cache_size);
            _cache.erase(_cache_keys[idx]);
            _cache[cache_key] = handle;
            _cache_keys[idx].swap(cache_key);
        } else {
            _cache[cache_key] = handle;
            _cache_keys.push_back(std::move(cache_key));
        }
    }

    _creating_fs.erase(cache_key);
    _creation_cv.notify_all();

    return create_status;
}

std::string HdfsFileSystemCache::_hdfs_cache_key(const THdfsParams& hdfs_params,
                                                 const std::string& fs_name) {
    fmt::memory_buffer buffer;

    if (!fs_name.empty()) {
        fmt::format_to(std::back_inserter(buffer), "{}", fs_name);
    } else if (hdfs_params.__isset.fs_name) {
        fmt::format_to(std::back_inserter(buffer), "{}", hdfs_params.fs_name);
    }

    if (hdfs_params.__isset.user) {
        fmt::format_to(std::back_inserter(buffer), "{}", hdfs_params.user);
    }

    return fmt::to_string(buffer);
}
} // namespace io
} // namespace doris
