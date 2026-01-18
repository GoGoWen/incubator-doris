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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.FileCacheKey;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.FileCacheValue;
import org.apache.doris.fs.FileSystemCache;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.BDPAuthContext;
import org.apache.doris.thrift.TUniqueId;

import com.alibaba.ttl.threadpool.TtlExecutors;
import com.google.common.collect.Lists;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.concurrent.ExecutorService;

public class HiveMetaStoreCacheTest {

    @BeforeClass
    public static void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

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
        HiveMetaStoreCache cache = new HiveMetaStoreCache(catalog, executor, executor);
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

    @Test
    public void testGetFilesByPartitionsWithoutCache(@Injectable HMSExternalCatalog catalog,
            @Injectable Env env, @Injectable ExternalMetaCacheMgr externalMetaCacheMgr) {
        Config.file_listing_max_second = 1;
        ExecutorService executor = TtlExecutors.getTtlExecutorService(ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "FileListingExecutor", Config.file_listing_max_second, true));
        new MockUp<HiveMetaStoreCache>() {
            @Mock
            void init() {
            }

            @Mock
            void initMetrics() {
            }
        };
        new MockUp<Logger>() {
            @Mock
            boolean isDebugEnabled() {
                return true;
            }
        };

        new MockUp<HiveMetaStoreCache>() {
            @Mock
            public FileCacheValue loadFiles(FileCacheKey key) {
                try {
                    Thread.sleep(2000);
                } catch (Exception e) {
                    // just do nothing
                }
                return new FileCacheValue();
            }
        };

        new MockUp<Env>() {
            @Mock
            Env getCurrentEnv() {
                return env;
            }
        };

        new Expectations() {
            {
                env.getExtMetaCacheMgr();
                result = externalMetaCacheMgr;
                minTimes = 0;

                externalMetaCacheMgr.getFileListingExecutor(anyInt);
                result = executor;
                minTimes = 0;
            }
        };

        HiveMetaStoreCache cache = new HiveMetaStoreCache(catalog, executor, executor);
        BDPAuthContext bdpAuthContext = new BDPAuthContext();
        TUniqueId queryId = new TUniqueId(111L, 222L);
        bdpAuthContext.setQueryId(queryId);
        bdpAuthContext.setErp("test_erp");
        bdpAuthContext.setSource("test_source");
        bdpAuthContext.setHadoopUserName("test_user");
        bdpAuthContext.setUserToken("test_token");
        bdpAuthContext.setBusinessLine("test_business");
        bdpAuthContext.setThreadLocalInfo();
        try {
            // Capture initial metric counts before the test
            long initialFileNumCount = MetricRepo.HISTO_HIVE_FILE_NUM.getCount();
            long initialLatencyCount = MetricRepo.HISTO_HIVE_FILE_LISTING_LATENCY.getCount();
            List<FileCacheValue> fileCacheValues = cache.getFilesByPartitionsWithoutCache(Lists.newArrayList(), "test");
            Assertions.assertTrue(fileCacheValues.isEmpty());
            // Check that metrics increased by 1
            Assertions.assertEquals(initialFileNumCount + 1, MetricRepo.HISTO_HIVE_FILE_NUM.getCount());
            Assertions.assertEquals(initialLatencyCount + 1, MetricRepo.HISTO_HIVE_FILE_LISTING_LATENCY.getCount());
        } finally {
            BDPAuthContext.clear();
        }
    }

    @Test
    public void testGetFilesByPartitionsTimeoutException(@Injectable HMSExternalCatalog catalog) {
        // Set a very short timeout to trigger TimeoutException (lines 717-723)
        int originalTimeout = Config.file_listing_max_second;
        Config.file_listing_max_second = 1;

        ExecutorService executor = TtlExecutors.getTtlExecutorService(ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "FileListingExecutorTimeout", Config.file_listing_max_second, true));

        new MockUp<HiveMetaStoreCache>() {
            @Mock
            void init() {
            }

            @Mock
            void initMetrics() {
            }
        };

        new MockUp<HiveMetaStoreCache>() {
            @Mock
            public FileCacheValue loadFiles(FileCacheKey key) {
                try {
                    // Sleep longer than the timeout to trigger TimeoutException
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    // Expected when future is cancelled
                }
                return new FileCacheValue();
            }
        };

        new Expectations() {
            {
                catalog.getName();
                result = "test_catalog";
            }
        };

        HiveMetaStoreCache cache = new HiveMetaStoreCache(catalog, executor, executor);

        // Set up BDPAuthContext
        BDPAuthContext bdpAuthContext = new BDPAuthContext();
        TUniqueId queryId = new TUniqueId(333L, 444L);
        bdpAuthContext.setQueryId(queryId);
        bdpAuthContext.setHadoopUserName("test_user");
        bdpAuthContext.setUserToken("test_token");
        bdpAuthContext.setThreadLocalInfo();

        // Create a partition to trigger file listing
        HivePartition partition = new HivePartition("testDb", "testTable", false,
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "hdfs://ns6666/user/hive/warehouse/test.db/test_table/a=dd",
                Lists.newArrayList("dd"), null);

        try {
            cache.getFilesByPartitionsWithoutCache(Lists.newArrayList(partition), "test");
            Assertions.fail("Expected CacheException due to timeout");
        } catch (CacheException e) {
            // Verify the exception message contains timeout information (lines 722-723)
            Assertions.assertTrue(e.getMessage().contains("timeout exception"),
                    "Exception message should mention timeout: " + e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("test_catalog"),
                    "Exception message should contain catalog name: " + e.getMessage());
        } finally {
            Config.file_listing_max_second = originalTimeout;
            BDPAuthContext.clear();
        }
    }

    @Test
    public void testGetFilesByPartitionsExecutionException(@Injectable HMSExternalCatalog catalog) {
        // Test ExecutionException handling (lines 727-729)
        ExecutorService executor = TtlExecutors.getTtlExecutorService(ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "FileListingExecutorExec", 60, true));

        new MockUp<HiveMetaStoreCache>() {
            @Mock
            void init() {
            }

            @Mock
            void initMetrics() {
            }
        };

        new MockUp<HiveMetaStoreCache>() {
            @Mock
            public FileCacheValue loadFiles(FileCacheKey key) {
                // Throw RuntimeException to cause ExecutionException
                throw new RuntimeException("Simulated file loading error");
            }
        };

        new Expectations() {
            {
                catalog.getName();
                result = "test_catalog_exec";
            }
        };

        HiveMetaStoreCache cache = new HiveMetaStoreCache(catalog, executor, executor);

        // Set up BDPAuthContext
        BDPAuthContext bdpAuthContext = new BDPAuthContext();
        TUniqueId queryId = new TUniqueId(555L, 666L);
        bdpAuthContext.setQueryId(queryId);
        bdpAuthContext.setHadoopUserName("test_user");
        bdpAuthContext.setUserToken("test_token");
        bdpAuthContext.setThreadLocalInfo();

        // Create a partition to trigger file listing
        HivePartition partition = new HivePartition("testDb", "testTable", false,
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "hdfs://ns6666/user/hive/warehouse/test.db/test_table/a=dd",
                Lists.newArrayList("dd"), null);

        try {
            cache.getFilesByPartitionsWithoutCache(Lists.newArrayList(partition), "test");
            Assertions.fail("Expected CacheException due to ExecutionException");
        } catch (CacheException e) {
            // Verify the exception message (lines 728-729)
            Assertions.assertTrue(e.getMessage().contains("failed to get files from partitions"),
                    "Exception message should mention failure: " + e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("test_catalog_exec"),
                    "Exception message should contain catalog name: " + e.getMessage());
        } finally {
            BDPAuthContext.clear();
        }
    }

    @Test
    public void testGetFilesByPartitionsInterruptedException(@Injectable HMSExternalCatalog catalog) {
        // Test InterruptedException handling (lines 730-732)
        ExecutorService executor = TtlExecutors.getTtlExecutorService(ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "FileListingExecutorInterrupt", 60, true));

        new MockUp<HiveMetaStoreCache>() {
            @Mock
            void init() {
            }

            @Mock
            void initMetrics() {
            }
        };

        new MockUp<HiveMetaStoreCache>() {
            @Mock
            public FileCacheValue loadFiles(FileCacheKey key) {
                try {
                    // Sleep to allow interruption
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted", e);
                }
                return new FileCacheValue();
            }
        };

        new Expectations() {
            {
                catalog.getName();
                result = "test_catalog_interrupt";
            }
        };

        HiveMetaStoreCache cache = new HiveMetaStoreCache(catalog, executor, executor);

        // Set up BDPAuthContext
        BDPAuthContext bdpAuthContext = new BDPAuthContext();
        TUniqueId queryId = new TUniqueId(777L, 888L);
        bdpAuthContext.setQueryId(queryId);
        bdpAuthContext.setHadoopUserName("test_user");
        bdpAuthContext.setUserToken("test_token");
        bdpAuthContext.setThreadLocalInfo();

        // Create a partition to trigger file listing
        HivePartition partition = new HivePartition("testDb", "testTable", false,
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "hdfs://ns6666/user/hive/warehouse/test.db/test_table/a=dd",
                Lists.newArrayList("dd"), null);

        // Create a thread that will be interrupted
        Thread testThread = new Thread(() -> {
            try {
                cache.getFilesByPartitionsWithoutCache(Lists.newArrayList(partition), "test");
                Assertions.fail("Expected CacheException due to InterruptedException");
            } catch (CacheException e) {
                // Verify the exception message (lines 731-732)
                Assertions.assertTrue(e.getMessage().contains("interrupted exception")
                        || e.getMessage().contains("failed to get files"),
                        "Exception message should mention interruption: " + e.getMessage());
            }
        });

        testThread.start();

        try {
            // Give the thread time to start
            Thread.sleep(100);
            // Interrupt the thread
            testThread.interrupt();
            // Wait for the thread to complete
            testThread.join(5000);
        } catch (InterruptedException e) {
            // Ignore
        } finally {
            BDPAuthContext.clear();
        }
    }

    @Test
    public void testGetFilesByPartitionsDebugLogging(@Injectable HMSExternalCatalog catalog,
            @Injectable Env env, @Injectable ExternalMetaCacheMgr externalMetaCacheMgr) {
        // Test debug logging (lines 739-741)
        ExecutorService executor = TtlExecutors.getTtlExecutorService(ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "FileListingExecutorDebug", 60, true));

        new MockUp<HiveMetaStoreCache>() {
            @Mock
            void init() {
            }

            @Mock
            void initMetrics() {
            }
        };

        // Enable debug logging
        new MockUp<Logger>() {
            @Mock
            boolean isDebugEnabled() {
                return true;
            }
        };

        new MockUp<HiveMetaStoreCache>() {
            @Mock
            public FileCacheValue loadFiles(FileCacheKey key) {
                FileCacheValue value = new FileCacheValue();
                // Add some files to verify the count in debug log
                RemoteFile file1 = new RemoteFile("file1", true, 1024, 32);
                file1.setPath(new Path("hdfs://ns6666/user/hive/warehouse/test.db/test_table/a=dd/file1.parquet"));
                RemoteFile file2 = new RemoteFile("file2", true, 2048, 32);
                file2.setPath(new Path("hdfs://ns6666/user/hive/warehouse/test.db/test_table/a=dd/file2.parquet"));
                value.addFile(file1, new LocationPath("hdfs://ns6666/user/hive/warehouse/test.db/test_table/a=dd/file1.parquet"));
                value.addFile(file2, new LocationPath("hdfs://ns6666/user/hive/warehouse/test.db/test_table/a=dd/file2.parquet"));
                return value;
            }
        };

        new MockUp<Env>() {
            @Mock
            Env getCurrentEnv() {
                return env;
            }
        };

        new Expectations() {
            {
                env.getExtMetaCacheMgr();
                result = externalMetaCacheMgr;
                minTimes = 0;

                externalMetaCacheMgr.getFileListingExecutor(anyInt);
                result = executor;
                minTimes = 0;

                catalog.getName();
                result = "test_catalog_debug";
                minTimes = 0;
            }
        };

        HiveMetaStoreCache cache = new HiveMetaStoreCache(catalog, executor, executor);

        // Set up BDPAuthContext
        BDPAuthContext bdpAuthContext = new BDPAuthContext();
        TUniqueId queryId = new TUniqueId(999L, 1000L);
        bdpAuthContext.setQueryId(queryId);
        bdpAuthContext.setHadoopUserName("test_user");
        bdpAuthContext.setUserToken("test_token");
        bdpAuthContext.setThreadLocalInfo();

        // Create partitions to trigger file listing
        HivePartition partition1 = new HivePartition("testDb", "testTable", false,
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "hdfs://ns6666/user/hive/warehouse/test.db/test_table/a=dd",
                Lists.newArrayList("dd"), null);
        HivePartition partition2 = new HivePartition("testDb", "testTable", false,
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "hdfs://ns6666/user/hive/warehouse/test.db/test_table/a=ee",
                Lists.newArrayList("ee"), null);

        try {
            List<FileCacheValue> result = cache.getFilesByPartitionsWithoutCache(
                    Lists.newArrayList(partition1, partition2), "test");
            // Verify files were returned (debug log would show file count)
            Assertions.assertEquals(2, result.size());
            // Each partition should have 2 files
            Assertions.assertEquals(2, result.get(0).getFiles().size());
            Assertions.assertEquals(2, result.get(1).getFiles().size());
        } finally {
            BDPAuthContext.clear();
        }
    }
}
