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

package org.apache.doris.datasource.hive;

import org.apache.doris.backup.Status;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.FileCacheValue;
import org.apache.doris.fs.FileSystemCache;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.qe.BDPAuthContext;

import com.google.common.collect.Lists;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.concurrent.ExecutorService;

public class HiveMetaStoreCacheTest {
    @Test
    public void testGetFileCache(@Injectable HMSExternalCatalog catalog, @Injectable ExecutorService executor,
            @Injectable Env env, @Injectable ExternalMetaCacheMgr externalMetaCacheMgr,
            @Injectable FileSystemCache fileSystemCache, @Injectable RemoteFileSystem remoteFileSystem) {
        new MockUp<HiveMetaStoreCache>() {
            @Mock
            void init() {
            }

            @Mock
            void initMetrics() {
            }
        };
        HiveMetaStoreCache cache = new HiveMetaStoreCache(catalog, executor, executor, executor);
        Config.enable_list_hdfs_files_recursively = true;
        Config.enable_list_hdfs_files_ignore_hidden_directory = true;
        String location = "hdfs://ns6666/user/hive/warehouse/test.db/test_table/a=dd/c=xx";
        String inputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
        List<String> partitionValues = Lists.newArrayList("dd", "xx");
        new MockUp<Env>() {
            @Mock
            Env getCurrentEnv() {
                return env;
            }
        };
        BDPAuthContext authContext = new BDPAuthContext("test", "test", "test", "xxxxxx");
        authContext.setThreadLocalInfo();
        FileSystemCache.FileSystemCacheKey key = new FileSystemCache.FileSystemCacheKey(authContext.getHadoopUserName(),
                authContext.getUserToken(), LocationPath.getFSIdentity(location, null),
                catalog.getCatalogProperty().getProperties(), null, new JobConf());
        List<RemoteFile> remoteFiles = Lists.newArrayList();
        try {
            new Expectations() {
                {
                    env.getExtMetaCacheMgr();
                    result = externalMetaCacheMgr;

                    externalMetaCacheMgr.getFsCache();
                    result = fileSystemCache;

                    fileSystemCache.getRemoteFileSystem(key);
                    result = remoteFileSystem;

                    remoteFileSystem.listFiles(location, true, remoteFiles);
                    result = new Delegate() {
                        public Status listFiles(String location, boolean recursive, List<RemoteFile> resultFiles) {
                            resultFiles.add(new RemoteFile(
                                    new Path("hdfs://ns6666/user/hive/warehouse/test.db/test_table/a=dd/c=xx/1.part"), false,
                                    1024, 32, System.currentTimeMillis(), null));
                            resultFiles.add(new RemoteFile(
                                    new Path("hdfs://ns6666/user/hive/warehouse/test.db/test_table/a=dd/c=xx/2.part"), false,
                                    2048, 32, System.currentTimeMillis(), null));
                            return Status.OK;
                        }
                    };
                }
            };
            FileCacheValue fileCacheValue = cache.getFileCache(location, inputFormat, new JobConf(),
                    partitionValues, null);
            Assertions.assertEquals(2, fileCacheValue.getFiles().size());
            Assertions.assertEquals(1024, fileCacheValue.getFiles().get(0).getLength());
            Assertions.assertEquals(2048, fileCacheValue.getFiles().get(1).getLength());
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testLzoIndexFileIsNotVisible() {
        Path lzoIndexPath = new Path("hdfs://namenode:8020/warehouse/table/data.lzo.index");
        Assertions.assertFalse(FileCacheValue.isFileVisible(lzoIndexPath),
                ".lzo.index files should not be visible");

        Path partitionLzoIndex = new Path("hdfs://namenode:8020/warehouse/table/dt=2025-01-01/file.lzo.index");
        Assertions.assertFalse(FileCacheValue.isFileVisible(partitionLzoIndex),
                ".lzo.index files in partition directories should not be visible");

        Path lzoDataFile = new Path("hdfs://namenode:8020/warehouse/table/data.lzo");
        Assertions.assertTrue(FileCacheValue.isFileVisible(lzoDataFile),
                ".lzo data files should be visible");

        Path otherIndexFile = new Path("hdfs://namenode:8020/warehouse/table/data.index");
        Assertions.assertTrue(FileCacheValue.isFileVisible(otherIndexFile),
                "Non .lzo.index files should be visible");

        Path parquetFile = new Path("hdfs://namenode:8020/warehouse/table/data.parquet");
        Assertions.assertTrue(FileCacheValue.isFileVisible(parquetFile),
                "Parquet files should be visible");

        Assertions.assertFalse(FileCacheValue.isFileVisible(null),
                "Null path should not be visible");

        Path hiddenFile = new Path("hdfs://namenode:8020/warehouse/table/.hidden");
        Assertions.assertFalse(FileCacheValue.isFileVisible(hiddenFile),
                "Hidden files starting with . should not be visible");

        Path underscoreFile = new Path("hdfs://namenode:8020/warehouse/table/_temporary");
        Assertions.assertFalse(FileCacheValue.isFileVisible(underscoreFile),
                "Files starting with _ should not be visible");
    }
}
