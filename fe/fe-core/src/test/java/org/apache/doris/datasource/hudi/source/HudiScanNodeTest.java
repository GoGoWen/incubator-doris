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

package org.apache.doris.datasource.hudi.source;

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.datasource.hive.source.HiveScanNode;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.planner.ListPartitionPrunerV2;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TFileRangeDesc;

import alluxio.core.client.runtime.com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.common.storage.HoodieDefaultStorageStrategy;
import org.apache.hudi.common.storage.HoodieStorageStrategy;
import org.apache.hudi.common.storage.HoodieStorageStrategyFactory;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.org.apache.avro.Schema;
import org.apache.hudi.org.apache.avro.Schema.Field;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class HudiScanNodeTest {

    @BeforeClass
    public static void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    @Test
    public void testDoInitialize(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc, @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog, @Injectable HoodieTableMetaClient client,
            @Injectable HoodieTableConfig tableConfig,
            @Injectable org.apache.hadoop.hive.metastore.api.Table hiveTable,
            @Injectable StorageDescriptor storageDescriptor, @Injectable SerDeInfo serDeInfo) {
        Map<String, String> para1 = Maps.newHashMap();
        para1.put("hoodie.query.without.cache.layer.enabled", "true");
        Map<String, String> para2 = Maps.newHashMap();
        para2.put("hoodie.datasource.write.recordkey.field", "id,name");
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                table.isHoodieCowTable();
                result = true;

                catalog.bindBrokerName();
                result = "test";

                table.useHiveSyncPartition();
                result = true;

                table.isView();
                result = false;

                client.reloadActiveTimeline();

                client.getTableConfig();
                result = tableConfig;

                tableConfig.getChubaoFsOwner();
                result = "test:test";

                table.getRemoteTable();
                result = hiveTable;

                hiveTable.getSd();
                result = storageDescriptor;

                storageDescriptor.getLocation();
                result = "hdfs://test";

                storageDescriptor.getInputFormat();
                result = "org.apache.hudi.hadoop.HoodieParquetInputFormat";

                storageDescriptor.getSerdeInfo();
                result =  serDeInfo;

                serDeInfo.getSerializationLib();
                result = "org.apache.hudi.hive.HoodieHiveSerDe";

                serDeInfo.getParameters();
                result = para1;

                hiveTable.getParameters();
                result = para2;
            }
        };
        new MockUp<HudiScanNode>() {
            @Mock
            public void computeColumnsFilter() {

            }

            @Mock
            public void initBackendPolicy() {

            }

            @Mock
            public void initSchemaParams() {

            }

            @Mock
            public TableSnapshot getQueryTableSnapshot() {
                return null;
            }

        };

        new MockUp<HiveMetaStoreClientHelper>() {
            @Mock
            public HoodieTableMetaClient getHudiClient(HMSExternalTable table)  {
                return client;
            }
        };

        new MockUp<HoodieStorageStrategyFactory>() {
            @Mock
            public HoodieStorageStrategy getInstant(HoodieTableMetaClient client, boolean reset) {
                return new HoodieDefaultStorageStrategy("", "", Option.of(client));
            }
        };

        MockedStatic<HoodieStorageStrategyFactory> mocked = Mockito.mockStatic(HoodieStorageStrategyFactory.class);
        mocked.when(() -> HoodieStorageStrategyFactory.getInstant(Mockito.any(), Mockito.anyBoolean())).thenReturn(
                new HoodieDefaultStorageStrategy("hdfs://ns10000/test",
                        "hdfs://ns10000/test/test", Option.of(client)));

        new MockUp<TableSchemaResolver>() {
            @Mock
            public Schema getTableAvroSchema() {
                List<Field> fieldList = new ArrayList<>();
                return Schema.createRecord(fieldList);
            }
        };
        try {
            HudiScanNode scanNode = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                    false, Optional.empty(), Optional.empty(), sessionVariable);
            scanNode.doInitialize();
            String[] primaryKeys = new String[]{"id", "name"};
            Assertions.assertEquals(primaryKeys.length, scanNode.getPrimaryKeys().size());
            Assertions.assertEquals(primaryKeys[0], scanNode.getPrimaryKeys().get(0));
            Assertions.assertEquals(primaryKeys[1], scanNode.getPrimaryKeys().get(1));
            Assertions.assertEquals("test", scanNode.getChubaoFsOwner());
        } catch (Exception e) {
            Assertions.assertEquals("", ExceptionUtils.getStackTrace(e));
            Assertions.fail(e);
        }
    }

    @Test
    public void testSetFsNameForRangeDesc(@Injectable SessionVariable sessionVariable,
                                          @Injectable TupleDescriptor tupleDesc, @Injectable HMSExternalTable table,
                                          @Injectable ExternalCatalog catalog) {
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                table.isHoodieCowTable();
                result = true;

                catalog.bindBrokerName();
                result = "test";

                table.useHiveSyncPartition();
                result = true;
            }
        };
        HudiScanNode scanNode = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                false, Optional.empty(), Optional.empty(), sessionVariable);
        scanNode.setChubaoFsOwner("test");
        FileSplit chubaoFileSplit = new FileSplit(new LocationPath(
                    "chubaofs://CHUBAO1101/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5.snappy.orc"),
                0, 112140970, 112140970, 0, null, Collections.emptyList());
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        scanNode.setFsNameForRangeDesc(chubaoFileSplit, rangeDesc);
        Assertions.assertEquals("chubaofs://test@CHUBAO1101", rangeDesc.getFsName());

        FileSplit hdfsFileSplit = new FileSplit(new LocationPath(
                    "hdfs://HDFSO1101/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e77f7d8.snappy.orc"),
                0, 112140970, 112140970, 0, null, Collections.emptyList());
        scanNode.setFsNameForRangeDesc(hdfsFileSplit, rangeDesc);
        Assertions.assertEquals("hdfs://HDFSO1101", rangeDesc.getFsName());
    }

    // Helper method to create mock HivePartition
    private HivePartition createMockHivePartition(String path, List<String> partitionValues) {
        return new HivePartition("testDb", "testTable", false,
                "org.apache.hudi.hadoop.HoodieParquetInputFormat", path, partitionValues, Maps.newHashMap());
    }

    // Helper method to create mock FileStatus
    private FileStatus createMockFileStatus(String path, long length) {
        FileStatus status = Mockito.mock(FileStatus.class);
        Mockito.when(status.getPath()).thenReturn(new Path(path));
        Mockito.when(status.getLen()).thenReturn(length);
        return status;
    }

    // Helper method to create mock HudiScanNode
    private HudiScanNode createMockHudiScanNode(SessionVariable sessionVariable, TupleDescriptor tupleDesc,
                                                 HMSExternalTable table, ExternalCatalog catalog,
                                                 HoodieTableMetaClient client) {
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                table.isHoodieCowTable();
                result = true;

                catalog.bindBrokerName();
                result = "test";

                table.useHiveSyncPartition();
                result = false;
            }
        };

        HudiScanNode scanNode = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                false, Optional.empty(), Optional.empty(), sessionVariable);

        // Set internal fields via reflection if needed
        try {
            java.lang.reflect.Field clientField = HudiScanNode.class.getDeclaredField("hudiClient");
            clientField.setAccessible(true);
            clientField.set(scanNode, client);

            java.lang.reflect.Field basePathField = HudiScanNode.class.getDeclaredField("basePath");
            basePathField.setAccessible(true);
            basePathField.set(scanNode, "/test/base/path");

            java.lang.reflect.Field queryInstantField = HudiScanNode.class.getDeclaredField("queryInstant");
            queryInstantField.setAccessible(true);
            queryInstantField.set(scanNode, "20240101000000");

            java.lang.reflect.Field partitionInitField = HiveScanNode.class.getDeclaredField("partitionInit");
            partitionInitField.setAccessible(true);
            partitionInitField.set(scanNode, true);

            java.lang.reflect.Field prunedPartitionsField = HiveScanNode.class.getDeclaredField("prunedPartitions");
            prunedPartitionsField.setAccessible(true);
            prunedPartitionsField.set(scanNode, new ArrayList<HivePartition>());

            // Mock timeline to avoid NullPointerException
            org.apache.hudi.common.table.timeline.HoodieTimeline mockTimeline =
                    org.mockito.Mockito.mock(org.apache.hudi.common.table.timeline.HoodieTimeline.class);
            org.apache.hudi.common.table.timeline.HoodieTimeline mockWriteTimeline =
                    org.mockito.Mockito.mock(org.apache.hudi.common.table.timeline.HoodieTimeline.class);
            org.apache.hudi.common.table.timeline.HoodieTimeline mockReplacedTimeline =
                    org.mockito.Mockito.mock(org.apache.hudi.common.table.timeline.HoodieTimeline.class);

            // Mock getInstantsAsStream to return empty stream
            org.mockito.Mockito.when(mockTimeline.getInstantsAsStream()).thenReturn(java.util.stream.Stream.empty());
            org.mockito.Mockito.when(mockWriteTimeline.getInstantsAsStream()).thenReturn(java.util.stream.Stream.empty());
            org.mockito.Mockito.when(mockReplacedTimeline.getInstantsAsStream()).thenReturn(java.util.stream.Stream.empty());

            // Mock getInstants to return empty list
            org.mockito.Mockito.when(mockTimeline.getInstants()).thenReturn(java.util.Collections.emptyList());
            org.mockito.Mockito.when(mockWriteTimeline.getInstants()).thenReturn(java.util.Collections.emptyList());
            org.mockito.Mockito.when(mockReplacedTimeline.getInstants()).thenReturn(java.util.Collections.emptyList());

            // Mock timeline relationships
            org.mockito.Mockito.when(mockTimeline.getWriteTimeline()).thenReturn(mockWriteTimeline);
            org.mockito.Mockito.when(mockTimeline.getCompletedReplaceTimeline()).thenReturn(mockReplacedTimeline);
            org.mockito.Mockito.when(mockWriteTimeline.getCompletedReplaceTimeline()).thenReturn(mockReplacedTimeline);

            java.lang.reflect.Field timelineField = HudiScanNode.class.getDeclaredField("timeline");
            timelineField.setAccessible(true);
            timelineField.set(scanNode, mockTimeline);
        } catch (Exception e) {
            // Handle exception
        }

        return scanNode;
    }


    @Test
    public void testGetSplitsExceedsMaxFileSize(@Injectable SessionVariable sessionVariable,
                                                @Injectable TupleDescriptor tupleDesc,
                                                @Injectable HMSExternalTable table,
                                                @Injectable ExternalCatalog catalog,
                                                @Injectable HoodieTableMetaClient client) throws Exception {

        new Expectations() {
            {
                client.getBasePathV2();
                result = new Path("/test/base/path");
                minTimes = 0;
            }
        };

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Mock static methods that cause NullPointerException
        MockedStatic<org.apache.hudi.common.util.ReflectionUtils> reflectionUtilsMock =
                Mockito.mockStatic(org.apache.hudi.common.util.ReflectionUtils.class);
        MockedStatic<org.apache.hudi.common.bootstrap.index.BootstrapIndex> bootstrapIndexMock =
                Mockito.mockStatic(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class);

        // Set a very low max file size limit
        long oldMaxFileSize = Config.max_selected_total_file_size_for_lakehouse_table;
        Config.max_selected_total_file_size_for_lakehouse_table = 1000; // 1KB for testing

        try {
            // Mock ReflectionUtils to handle null class names
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.getClass(Mockito.any()))
                    .thenReturn(Object.class);
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.loadClass(Mockito.any()))
                    .thenReturn(Object.class);

            // Mock BootstrapIndex to return a mock instance
            bootstrapIndexMock.when(() -> org.apache.hudi.common.bootstrap.index.BootstrapIndex.getBootstrapIndex(Mockito.any()))
                    .thenReturn(Mockito.mock(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class));
            // Create partitions
            List<HivePartition> partitions = Arrays.asList(
                    createMockHivePartition("/test/base/path/partition1", Arrays.asList("2024", "01"))
            );

            java.lang.reflect.Field prunedPartitionsField = HiveScanNode.class.getDeclaredField("prunedPartitions");
            prunedPartitionsField.setAccessible(true);
            prunedPartitionsField.set(scanNode, partitions);

            // HoodieTableFileSystemView is mocked globally in static block

            // Mock ConnectContext
            new MockUp<ConnectContext>() {
                @Mock
                public ConnectContext get() {
                    ConnectContext context = Mockito.mock(ConnectContext.class);
                    return context;
                }
            };

            // Mock UserGroupInformation
            new MockUp<UserGroupInformation>() {
                @Mock
                public UserGroupInformation createRemoteUser(String user, String cluster, String token) {
                    return Mockito.mock(UserGroupInformation.class);
                }

                @Mock
                public <T> T doAs(PrivilegedAction<T> action) {
                    return action.run();
                }
            };

            // Mock FileSystem with large files
            FileSystem fs = Mockito.mock(FileSystem.class);
            FileStatus[] fileStatuses = new FileStatus[] {
                createMockFileStatus("/test/base/path/partition1/file1.parquet", 10000), // 10KB
                createMockFileStatus("/test/base/path/partition1/file2.parquet", 20000)  // 20KB
            };
            Mockito.when(fs.listStatus(Mockito.any(Path.class))).thenReturn(fileStatuses);

            new MockUp<Path>() {
                @Mock
                public FileSystem getFileSystem(Configuration conf) {
                    return fs;
                }
            };

            // Mock storageStrategy
            HoodieStorageStrategy storageStrategy = Mockito.mock(HoodieStorageStrategy.class);
            java.lang.reflect.Field storageStrategyField = HudiScanNode.class.getDeclaredField("storageStrategy");
            storageStrategyField.setAccessible(true);
            storageStrategyField.set(scanNode, storageStrategy);

            Mockito.when(storageStrategy.getRelativePath(Mockito.any())).thenReturn("relative/path");
            Mockito.when(storageStrategy.getAllLocations(Mockito.anyString(), Mockito.anyBoolean()))
                    .thenReturn(new HashSet<>(Arrays.asList(new Path("/test/base/path/partition1"))));

            // Create mock base files with sizes that exceed the 1KB limit (total 30KB)
            org.apache.hudi.common.model.HoodieBaseFile baseFile1 = Mockito.mock(org.apache.hudi.common.model.HoodieBaseFile.class);
            org.apache.hudi.common.model.HoodieBaseFile baseFile2 = Mockito.mock(org.apache.hudi.common.model.HoodieBaseFile.class);

            Mockito.when(baseFile1.getPath()).thenReturn("/test/base/path/partition1/file1.parquet");
            Mockito.when(baseFile1.getFileSize()).thenReturn(15000L); // 15KB

            Mockito.when(baseFile2.getPath()).thenReturn("/test/base/path/partition1/file2.parquet");
            Mockito.when(baseFile2.getFileSize()).thenReturn(15000L); // 15KB

            // Mock HoodieTableFileSystemView constructor to return our mock
            new MockUp<org.apache.hudi.common.table.view.HoodieTableFileSystemView>() {
                @Mock
                public void $init(org.apache.hudi.common.table.HoodieTableMetaClient metaClient, // CHECKSTYLE IGNORE THIS LINE
                                  org.apache.hudi.common.table.timeline.HoodieTimeline timeline,
                                  org.apache.hudi.common.storage.HoodieStorageStrategy storageStrategy) {
                    // Constructor mock - no-op
                }

                @Mock
                public java.util.stream.Stream<org.apache.hudi.common.model.HoodieBaseFile> getLatestBaseFilesBeforeOrOn(String partitionPath, String maxCommitTime) {
                    // Create a new stream each time to avoid stream reuse issues
                    return java.util.stream.Stream.of(baseFile1, baseFile2);
                }
            };

            // Mock environment for file listing executor
            new MockUp<Env>() {
                @Mock
                public Env getCurrentEnv() {
                    Env env = Mockito.mock(Env.class);
                    org.apache.doris.datasource.ExternalMetaCacheMgr metaCacheMgr =
                            Mockito.mock(org.apache.doris.datasource.ExternalMetaCacheMgr.class);
                    java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();

                    Mockito.when(env.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);
                    Mockito.when(metaCacheMgr.getLakehouseGetPartitionSplitExecutor()).thenReturn(executor);
                    return env;
                }
            };

            // Mock table for error message
            new Expectations() {
                {
                    table.getDbName();
                    result = "testDb";

                    table.getName();
                    result = "testTable";
                }
            };

            // Execute and expect exception for line 465
            List<Split> splits = Collections.synchronizedList(new ArrayList<>());

            try {
                scanNode.getPartitionsSplits(partitions, splits);
                Assert.fail("Expected AnalysisException due to file size exceeding limit");
            } catch (AnalysisException e) {
                // Verify that the exception message indicates file size limit exceeded
                Assert.assertTrue("Exception message should mention exceed max bytes: " + e.getMessage(),
                        e.getMessage().contains("has exceed max bytes for single hudi table"));
                Assert.assertTrue("Exception message should contain table name: " + e.getMessage(),
                        e.getMessage().contains("testDb.testTable"));
            }
        } finally {
            Config.max_selected_total_file_size_for_lakehouse_table = oldMaxFileSize;
            // Clean up static mocks
            reflectionUtilsMock.close();
            bootstrapIndexMock.close();
        }
    }

    @Test
    public void testGetPartitionMetadata(@Injectable SessionVariable sessionVariable,
                                        @Injectable TupleDescriptor tupleDesc,
                                        @Injectable HMSExternalTable table,
                                        @Injectable ExternalCatalog catalog,
                                        @Injectable HoodieTableMetaClient client) throws Exception {


        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Set up storage strategy
        HoodieStorageStrategy storageStrategy = Mockito.mock(HoodieStorageStrategy.class);
        java.lang.reflect.Field storageStrategyField = HudiScanNode.class.getDeclaredField("storageStrategy");
        storageStrategyField.setAccessible(true);
        storageStrategyField.set(scanNode, storageStrategy);

        // Mock ConnectContext
        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                ConnectContext context = Mockito.mock(ConnectContext.class);
                return context;
            }
        };

        // Mock FileSystem
        FileSystem fs = Mockito.mock(FileSystem.class);
        FileStatus[] fileStatuses = new FileStatus[] {
            createMockFileStatus("/test/base/path/partition1/file1.parquet", 1000),
            createMockFileStatus("/test/base/path/partition1/file2.parquet", 2000)
        };
        Mockito.when(fs.listStatus(Mockito.any(Path.class))).thenReturn(fileStatuses);

        // Mock UserGroupInformation
        UserGroupInformation mockUgi = Mockito.mock(UserGroupInformation.class);
        new MockUp<UserGroupInformation>() {
            @Mock
            public UserGroupInformation createRemoteUser(String user, String cluster, String token) {
                return mockUgi;
            }
        };

        // Mock the doAs method to return the FileSystem directly
        try {
            Mockito.when(mockUgi.doAs(Mockito.any(PrivilegedAction.class))).thenReturn(fs);
        } catch (Exception e) {
            // Handle mocking exception
        }

        Mockito.when(storageStrategy.getRelativePath(Mockito.any())).thenReturn("relative/path");
        Mockito.when(storageStrategy.getAllLocations(Mockito.anyString(), Mockito.anyBoolean()))
                .thenReturn(new HashSet<>(Arrays.asList(new Path("/test/base/path/partition1"))));
        // Create test partition
        HivePartition partition = createMockHivePartition("/test/base/path/partition1", Arrays.asList("2024", "01"));

        // Use reflection to call getPartitionMetadata
        Method getPartitionMetadataMethod = HudiScanNode.class.getDeclaredMethod("getPartitionMetadata", HivePartition.class, org.apache.hadoop.fs.Path.class);
        getPartitionMetadataMethod.setAccessible(true);
        Object metadata = getPartitionMetadataMethod.invoke(scanNode, partition, new org.apache.hadoop.fs.Path("/test/base/path"));

        // Verify the metadata
        Assert.assertNotNull(metadata);

        // Access PartitionMetadata fields via reflection
        Class<?> partitionMetadataClass = metadata.getClass();
        java.lang.reflect.Field totalSizeField = partitionMetadataClass.getDeclaredField("totalSize");
        totalSizeField.setAccessible(true);
        long totalSize = (long) totalSizeField.get(metadata);
        Assert.assertEquals(0, totalSize);
    }


    @Test
    public void testGetSplitsWithIncrementalRead(@Injectable SessionVariable sessionVariable,
                                                 @Injectable TupleDescriptor tupleDesc,
                                                 @Injectable HMSExternalTable table,
                                                 @Injectable ExternalCatalog catalog,
                                                 @Injectable HoodieTableMetaClient client,
                                                 @Injectable IncrementalRelation incrementalRelation) throws Exception {

        // Create scan node with incremental read
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                table.isHoodieCowTable();
                result = true;

                catalog.bindBrokerName();
                result = "test";

                table.useHiveSyncPartition();
                result = false;
            }
        };

        HudiScanNode scanNode = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                false, Optional.empty(), Optional.of(incrementalRelation), sessionVariable);

        // Set incremental read flag
        java.lang.reflect.Field incrementalReadField = HudiScanNode.class.getDeclaredField("incrementalRead");
        incrementalReadField.setAccessible(true);
        incrementalReadField.set(scanNode, true);

        java.lang.reflect.Field incrementalRelationField = HudiScanNode.class.getDeclaredField("incrementalRelation");
        incrementalRelationField.setAccessible(true);
        incrementalRelationField.set(scanNode, incrementalRelation);

        // Mock incremental relation
        List<Split> mockSplits = Arrays.asList(
                Mockito.mock(Split.class),
                Mockito.mock(Split.class)
        );

        new Expectations() {
            {
                incrementalRelation.fallbackFullTableScan();
                result = false;

                incrementalRelation.collectSplits();
                result = mockSplits;
            }
        };
        // Execute
        List<Split> splits = scanNode.getSplits(3);

        // Verify
        Assert.assertEquals(2, splits.size());
    }

    @Test
    public void testIsBatchMode(@Injectable SessionVariable sessionVariable,
                                @Injectable TupleDescriptor tupleDesc,
                                @Injectable HMSExternalTable table,
                                @Injectable ExternalCatalog catalog,
                                @Injectable HoodieTableMetaClient client) throws Exception {

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Create many partitions to trigger batch mode
        List<HivePartition> partitions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            partitions.add(createMockHivePartition("/test/base/path/partition" + i, Arrays.asList("2024", String.valueOf(i))));
        }

        java.lang.reflect.Field prunedPartitionsField = HiveScanNode.class.getDeclaredField("prunedPartitions");
        prunedPartitionsField.setAccessible(true);
        prunedPartitionsField.set(scanNode, partitions);

        // Mock ConnectContext
        new MockUp<org.apache.doris.qe.ConnectContext>() {
            @Mock
            public org.apache.doris.qe.ConnectContext get() {
                org.apache.doris.qe.ConnectContext context = Mockito.mock(org.apache.doris.qe.ConnectContext.class);
                SessionVariable sv = Mockito.mock(SessionVariable.class);
                Mockito.when(context.getSessionVariable()).thenReturn(sv);
                Mockito.when(sv.getNumPartitionsInBatchMode()).thenReturn(5);
                return context;
            }
        };

        // Execute
        boolean isBatchMode = scanNode.isBatchMode();

        // Verify - 10 partitions >= 5 threshold
        Assert.assertTrue(isBatchMode);
    }

    @Test
    public void testStartSplit(@Injectable SessionVariable sessionVariable,
                               @Injectable TupleDescriptor tupleDesc,
                               @Injectable HMSExternalTable table,
                               @Injectable ExternalCatalog catalog,
                               @Injectable HoodieTableMetaClient client) throws Exception {

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Mock static methods that cause NullPointerException
        MockedStatic<org.apache.hudi.common.util.ReflectionUtils> reflectionUtilsMock =
                Mockito.mockStatic(org.apache.hudi.common.util.ReflectionUtils.class);
        MockedStatic<org.apache.hudi.common.bootstrap.index.BootstrapIndex> bootstrapIndexMock =
                Mockito.mockStatic(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class);

        try {
            // Mock ReflectionUtils to handle null class names
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.getClass(Mockito.any()))
                    .thenReturn(Object.class);
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.loadClass(Mockito.any()))
                    .thenReturn(Object.class);

            // Mock BootstrapIndex to return a mock instance
            bootstrapIndexMock.when(() -> org.apache.hudi.common.bootstrap.index.BootstrapIndex.getBootstrapIndex(Mockito.any()))
                    .thenReturn(Mockito.mock(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class));

            // Create test partitions
            List<HivePartition> partitions = Arrays.asList(
                    createMockHivePartition("/test/base/path/partition1", Arrays.asList("2024", "01"))
            );

            java.lang.reflect.Field prunedPartitionsField = HiveScanNode.class.getDeclaredField("prunedPartitions");
            prunedPartitionsField.setAccessible(true);
            prunedPartitionsField.set(scanNode, partitions);

            HoodieStorageStrategy storageStrategy = Mockito.mock(HoodieStorageStrategy.class);
            java.lang.reflect.Field storageStrategyField = HudiScanNode.class.getDeclaredField("storageStrategy");
            storageStrategyField.setAccessible(true);
            storageStrategyField.set(scanNode, storageStrategy);

            // HoodieTableFileSystemView is mocked globally in static block

            // Mock ConnectContext
            new MockUp<ConnectContext>() {
                @Mock
                public ConnectContext get() {
                    ConnectContext context = Mockito.mock(ConnectContext.class);
                    return context;
                }
            };

            // Mock UserGroupInformation
            new MockUp<UserGroupInformation>() {
                @Mock
                public UserGroupInformation createRemoteUser(String user, String cluster, String token) {
                    return Mockito.mock(UserGroupInformation.class);
                }

                @Mock
                public <T> T doAs(PrivilegedAction<T> action) {
                    return action.run();
                }
            };

            // Mock FileSystem
            FileSystem fs = Mockito.mock(FileSystem.class);
            FileStatus[] fileStatuses = new FileStatus[] {
                createMockFileStatus("/test/base/path/partition1/file1.parquet", 1000)
            };
            Mockito.when(fs.listStatus(Mockito.any(Path.class))).thenReturn(fileStatuses);

            new MockUp<Path>() {
                @Mock
                public FileSystem getFileSystem(Configuration conf) {
                    return fs;
                }
            };

            Mockito.when(storageStrategy.getRelativePath(Mockito.any())).thenReturn("relative/path");
            Mockito.when(storageStrategy.getAllLocations(Mockito.anyString(), Mockito.anyBoolean()))
                    .thenReturn(new HashSet<>(Arrays.asList(new Path("/test/base/path/partition1"))));

            // Execute startSplit
            scanNode.startSplit(3);

            // Wait a bit for async processing
            Thread.sleep(100);

            // Verify no exceptions were thrown
            java.lang.reflect.Field batchExceptionField = HudiScanNode.class.getDeclaredField("batchException");
            batchExceptionField.setAccessible(true);
            AtomicReference<UserException> batchException = (AtomicReference<UserException>) batchExceptionField.get(scanNode);
            Assert.assertNull(batchException.get());
        } finally {
            // Clean up static mocks
            reflectionUtilsMock.close();
            bootstrapIndexMock.close();
        }
    }

    @Test
    public void testEmptyPartitions(@Injectable SessionVariable sessionVariable,
                                    @Injectable TupleDescriptor tupleDesc,
                                    @Injectable HMSExternalTable table,
                                    @Injectable ExternalCatalog catalog,
                                    @Injectable HoodieTableMetaClient client) throws Exception {

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Mock static methods that cause NullPointerException
        MockedStatic<org.apache.hudi.common.util.ReflectionUtils> reflectionUtilsMock =
                Mockito.mockStatic(org.apache.hudi.common.util.ReflectionUtils.class);
        MockedStatic<org.apache.hudi.common.bootstrap.index.BootstrapIndex> bootstrapIndexMock =
                Mockito.mockStatic(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class);

        try {
            // Mock ReflectionUtils to handle null class names
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.getClass(Mockito.any()))
                    .thenReturn(Object.class);
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.loadClass(Mockito.any()))
                    .thenReturn(Object.class);

            // Mock BootstrapIndex to return a mock instance
            bootstrapIndexMock.when(() -> org.apache.hudi.common.bootstrap.index.BootstrapIndex.getBootstrapIndex(Mockito.any()))
                    .thenReturn(Mockito.mock(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class));

            // Set empty partitions
            List<HivePartition> partitions = new ArrayList<>();
            java.lang.reflect.Field prunedPartitionsField = HiveScanNode.class.getDeclaredField("prunedPartitions");
            prunedPartitionsField.setAccessible(true);
            prunedPartitionsField.set(scanNode, partitions);

            // Execute
            List<Split> splits = scanNode.getSplits(3);
            // Verify
            Assert.assertNotNull(splits);
            Assert.assertEquals(0, splits.size());
        } finally {
            // Clean up static mocks
            reflectionUtilsMock.close();
            bootstrapIndexMock.close();
        }

    }

    @Test
    public void testGetSplitsExceedsMaxPartitionNum(@Injectable SessionVariable sessionVariable,
                                                    @Injectable TupleDescriptor tupleDesc,
                                                    @Injectable HMSExternalTable table,
                                                    @Injectable ExternalCatalog catalog,
                                                    @Injectable HoodieTableMetaClient client) throws Exception {

        // Set a very low max partition limit first
        int oldMaxPartitionNum = Config.max_selected_partition_num_for_lakehouse_table;
        Config.max_selected_partition_num_for_lakehouse_table = 2; // Set to 2 for testing

        try {
            // Use the existing helper to create a properly mocked scan node
            HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

            // Override to make this a partitioned table and reset partitionInit
            new Expectations() {
                {
                    table.getPartitionColumnTypes();
                    result = Arrays.asList(Type.STRING, Type.STRING); // Non-empty list to indicate partitioned table

                    table.getDbName();
                    result = "testDb";

                    table.getName();
                    result = "testTable";
                }
            };

            // Reset partitionInit to false so partition processing runs
            java.lang.reflect.Field partitionInitField = HiveScanNode.class.getDeclaredField("partitionInit");
            partitionInitField.setAccessible(true);
            partitionInitField.set(scanNode, false);

            // Directly test the scenario by calling getPrunedPartitions method via reflection
            Method getPrunedPartitionsMethod = HudiScanNode.class.getDeclaredMethod("getPrunedPartitions",
                    HoodieTableMetaClient.class, Option.class);
            getPrunedPartitionsMethod.setAccessible(true);

            // This should trigger the constraint check and throw AnalysisException
            // We'll simulate having too many partitions by mocking the pruner to return more partitions than allowed
            new MockUp<ListPartitionPrunerV2>() {
                @Mock
                public void init(Map idToPartitionItem, List partitionColumns, Map columnNameToRange,
                        Map uidToPartitionRange, Map rangeToId, Map singleColumnRangeMap, boolean isPartitionColumnsAnalyzed) {
                    // Mock initialization
                }

                @Mock
                public Collection<Long> prune() {
                    // Return more partition IDs than the limit allows
                    return Arrays.asList(1L, 2L, 3L, 4L, 5L); // 5 partitions > limit of 2
                }
            };

            // Mock the partition processing components
            new MockUp<Env>() {
                @Mock
                public Env getCurrentEnv() {
                    Env env = Mockito.mock(Env.class);
                    org.apache.doris.datasource.ExternalMetaCacheMgr metaCacheMgr =
                            Mockito.mock(org.apache.doris.datasource.ExternalMetaCacheMgr.class);
                    HudiCachedPartitionProcessor processor = Mockito.mock(HudiCachedPartitionProcessor.class);

                    // Mock the partition processor to return partition values
                    try {
                        TablePartitionValues partitionValues = Mockito.mock(TablePartitionValues.class);

                        // Create partition data that exceeds the limit
                        Map<Long, PartitionItem> idToPartitionItem = Maps.newHashMap();
                        Map<Long, String> partitionIdToNameMap = Maps.newHashMap();
                        Map<Long, List<String>> partitionValuesMap = Maps.newHashMap();

                        // Add 5 partitions (more than the limit of 2)
                        for (long i = 1L; i <= 5L; i++) {
                            PartitionItem mockItem = Mockito.mock(PartitionItem.class);
                            idToPartitionItem.put(i, mockItem);
                            partitionIdToNameMap.put(i, "partition" + i);
                            partitionValuesMap.put(i, Arrays.asList("2024", String.valueOf(i)));
                        }

                        java.util.concurrent.locks.ReentrantReadWriteLock lock = new java.util.concurrent.locks.ReentrantReadWriteLock();
                        Mockito.when(partitionValues.readLock()).thenReturn(lock.readLock());
                        Mockito.when(partitionValues.getIdToPartitionItem()).thenReturn(idToPartitionItem);
                        Mockito.when(partitionValues.getPartitionIdToNameMap()).thenReturn(partitionIdToNameMap);
                        Mockito.when(partitionValues.getPartitionValuesMap()).thenReturn(partitionValuesMap);
                        Mockito.when(partitionValues.getUidToPartitionRange()).thenReturn(Maps.newHashMap());
                        Mockito.when(partitionValues.getRangeToId()).thenReturn(Maps.newHashMap());
                        Mockito.when(partitionValues.getSingleColumnRangeMap()).thenReturn(com.google.common.collect.TreeRangeMap.create());

                        Mockito.when(processor.getPartitionValues(Mockito.any(), Mockito.any(), Mockito.anyBoolean(), Mockito.anyBoolean()))
                                .thenReturn(partitionValues);
                    } catch (Exception e) {
                        // Handle mocking exception
                    }

                    Mockito.when(env.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);
                    Mockito.when(metaCacheMgr.getHudiPartitionProcess(Mockito.any())).thenReturn(processor);
                    return env;
                }
            };

            // Execute and expect AnalysisException due to partition count constraint
            // Since we're using reflection, the AnalysisException will be wrapped in InvocationTargetException
            try {
                getPrunedPartitionsMethod.invoke(scanNode, client, Option.empty());
                Assert.fail("Expected AnalysisException to be thrown due to partition count exceeding limit");
            } catch (java.lang.reflect.InvocationTargetException e) {
                // Unwrap the actual exception
                Throwable cause = e.getCause();
                Assert.assertTrue("Expected AnalysisException but got: " + cause.getClass().getName(),
                                cause instanceof AnalysisException);

                AnalysisException analysisException = (AnalysisException) cause;
                String message = analysisException.getMessage();

                // Verify the exception message contains the expected constraint violation details
                Assert.assertTrue("Exception message should mention partition count exceeding limit, but was: " + message,
                                message.contains("exceed max selected partition num"));
                Assert.assertTrue("Exception message should mention the table name, but was: " + message,
                                message.contains("testDb.testTable"));
            } catch (Exception e) {
                Assert.fail("Unexpected exception type: " + e.getClass().getName() + " - " + e.getMessage());
            }

        } finally {
            Config.max_selected_partition_num_for_lakehouse_table = oldMaxPartitionNum;
        }
    }

    @Test
    public void testNumApproximateSplits(@Injectable SessionVariable sessionVariable,
                                         @Injectable TupleDescriptor tupleDesc,
                                         @Injectable HMSExternalTable table,
                                         @Injectable ExternalCatalog catalog,
                                         @Injectable HoodieTableMetaClient client) throws Exception {

        // Create a simplified mock setup that doesn't include getCatalogProperties()
        // since numApproximateSplits() doesn't call it
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                table.isHoodieCowTable();
                result = true;

                catalog.bindBrokerName();
                result = "test";

                table.useHiveSyncPartition();
                result = false;

                // Don't set up getCatalogProperties() expectation since it's not needed
            }
        };

        HudiScanNode scanNode = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                false, Optional.empty(), Optional.empty(), sessionVariable);

        // Set partitions
        List<HivePartition> partitions = Arrays.asList(
                createMockHivePartition("/test/base/path/partition1", Arrays.asList("2024", "01")),
                createMockHivePartition("/test/base/path/partition2", Arrays.asList("2024", "02"))
        );

        java.lang.reflect.Field prunedPartitionsField = HiveScanNode.class.getDeclaredField("prunedPartitions");
        prunedPartitionsField.setAccessible(true);
        prunedPartitionsField.set(scanNode, partitions);

        // Set numSplitsPerPartition
        java.lang.reflect.Field numSplitsPerPartitionField = HudiScanNode.class.getDeclaredField("numSplitsPerPartition");
        numSplitsPerPartitionField.setAccessible(true);
        AtomicInteger numSplitsPerPartition = (AtomicInteger) numSplitsPerPartitionField.get(scanNode);
        numSplitsPerPartition.set(5);

        // Execute
        int approximateSplits = scanNode.numApproximateSplits();

        // Verify - 5 splits per partition * 2 partitions = 10
        Assert.assertEquals(10, approximateSplits);
    }

    @Test
    public void testPrunePartitionsInheritedFromParent(@Injectable SessionVariable sessionVariable,
                                                      @Injectable TupleDescriptor tupleDesc,
                                                      @Injectable HMSExternalTable table,
                                                      @Injectable ExternalCatalog catalog,
                                                      @Injectable HoodieTableMetaClient client) throws Exception {

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Verify that HudiScanNode can access inherited prunedPartitions from HiveScanNode
        java.lang.reflect.Field prunedPartitionsField = HiveScanNode.class.getDeclaredField("prunedPartitions");
        prunedPartitionsField.setAccessible(true);

        // Set pruned partitions using inherited field
        List<HivePartition> testPartitions = Arrays.asList(
                createMockHivePartition("/test/partition1", Arrays.asList("2024", "01")),
                createMockHivePartition("/test/partition2", Arrays.asList("2024", "02"))
        );
        prunedPartitionsField.set(scanNode, testPartitions);

        // Verify the field is accessible and data is set correctly
        @SuppressWarnings("unchecked")
        List<HivePartition> retrievedPartitions = (List<HivePartition>) prunedPartitionsField.get(scanNode);
        Assert.assertNotNull(retrievedPartitions);
        Assert.assertEquals(2, retrievedPartitions.size());
    }

    @Test
    public void testIsBatchMode(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc, @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog) {
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                table.isHoodieCowTable();
                result = true;

                catalog.bindBrokerName();
                result = "test";

                table.useHiveSyncPartition();
                result = true;

                sessionVariable.getNumPartitionsInBatchMode();
                result = 1;
            }
        };
        HudiScanNode scanNode = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                false, Optional.empty(), Optional.empty(), sessionVariable);
        new MockUp<HudiScanNode>() {
            @Mock
            public List<HivePartition> getPartitions() {
                HivePartition partition1 = new HivePartition("test", "test", false,
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "hdfs://hk-dev01:8121/user/doris/parquet/partition_table/nation=cn/city=beijing",
                        Lists.newArrayList("cn", "beijing"), Maps.newHashMap());
                HivePartition partition2 = new HivePartition("test", "test", false,
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "hdfs://hk-dev01:8121/user/doris/parquet/partition_table/nation=cn/city=shanghai",
                        Lists.newArrayList("cn", "shanghai"), Maps.newHashMap());
                return Lists.newArrayList(partition1, partition2);
            }
        };
        Assertions.assertTrue(scanNode.isBatchMode());
        new Expectations() {
            {
                sessionVariable.getNumPartitionsInBatchMode();
                result = 1024;
            }
        };
        Assertions.assertFalse(scanNode.isBatchMode());
        new MockUp<HudiScanNode>() {
            @Mock
            public List<HivePartition> getPartitions() {
                throw new RuntimeException("get partitions failed");
            }
        };
        HudiScanNode scanNode1 = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                false, Optional.empty(), Optional.empty(), sessionVariable);
        Assertions.assertFalse(scanNode1.isBatchMode());
    }

    @Test
    public void testCowTablePartitionProcessingWithBaseFiles(@Injectable SessionVariable sessionVariable,
                                                              @Injectable TupleDescriptor tupleDesc,
                                                              @Injectable HMSExternalTable table,
                                                              @Injectable ExternalCatalog catalog,
                                                              @Injectable HoodieTableMetaClient client) throws Exception {

        new Expectations() {
            {
                client.getBasePathV2();
                result = new Path("/test/base/path");
                minTimes = 0;
            }
        };

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Override to ensure this is a COW table
        java.lang.reflect.Field isCowOrRoTableField = HudiScanNode.class.getDeclaredField("isCowOrRoTable");
        isCowOrRoTableField.setAccessible(true);
        isCowOrRoTableField.set(scanNode, true);

        // Mock static methods that cause NullPointerException
        MockedStatic<org.apache.hudi.common.util.ReflectionUtils> reflectionUtilsMock =
                Mockito.mockStatic(org.apache.hudi.common.util.ReflectionUtils.class);
        MockedStatic<org.apache.hudi.common.bootstrap.index.BootstrapIndex> bootstrapIndexMock =
                Mockito.mockStatic(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class);

        try {
            // Mock ReflectionUtils to handle null class names
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.getClass(Mockito.any()))
                    .thenReturn(Object.class);
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.loadClass(Mockito.any()))
                    .thenReturn(Object.class);

            // Mock BootstrapIndex to return a mock instance
            bootstrapIndexMock.when(() -> org.apache.hudi.common.bootstrap.index.BootstrapIndex.getBootstrapIndex(Mockito.any()))
                    .thenReturn(Mockito.mock(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class));

            // Create test partition
            HivePartition partition = createMockHivePartition("/test/base/path/partition1", Arrays.asList("2024", "01"));
            List<HivePartition> partitions = Arrays.asList(partition);

            // Create mock base files with specific sizes
            org.apache.hudi.common.model.HoodieBaseFile baseFile1 = Mockito.mock(org.apache.hudi.common.model.HoodieBaseFile.class);
            org.apache.hudi.common.model.HoodieBaseFile baseFile2 = Mockito.mock(org.apache.hudi.common.model.HoodieBaseFile.class);

            Mockito.when(baseFile1.getPath()).thenReturn("/test/base/path/partition1/file1.parquet");
            Mockito.when(baseFile1.getFileSize()).thenReturn(1000L);

            Mockito.when(baseFile2.getPath()).thenReturn("/test/base/path/partition1/file2.parquet");
            Mockito.when(baseFile2.getFileSize()).thenReturn(2000L);

            // Mock the HoodieTableFileSystemView constructor and methods using MockUp
            new MockUp<org.apache.hudi.common.table.view.HoodieTableFileSystemView>() {
                @Mock
                public void $init(org.apache.hudi.common.table.HoodieTableMetaClient metaClient, // CHECKSTYLE IGNORE THIS LINE
                                  org.apache.hudi.common.table.timeline.HoodieTimeline timeline,
                                  org.apache.hudi.common.storage.HoodieStorageStrategy storageStrategy) {
                    // Constructor mock - no-op
                }

                @Mock
                public java.util.stream.Stream<org.apache.hudi.common.model.HoodieBaseFile> getLatestBaseFilesBeforeOrOn(String partitionPath, String maxCommitTime) {
                    // Return stream of mock base files for COW table processing (line 406)
                    return java.util.stream.Stream.of(baseFile1, baseFile2);
                }
            };

            // Set storage strategy
            HoodieStorageStrategy storageStrategy = Mockito.mock(HoodieStorageStrategy.class);
            java.lang.reflect.Field storageStrategyField = HudiScanNode.class.getDeclaredField("storageStrategy");
            storageStrategyField.setAccessible(true);
            storageStrategyField.set(scanNode, storageStrategy);

            // Create test splits list
            List<Split> splits = Collections.synchronizedList(new ArrayList<>());

            // Call the method that should execute line 406
            scanNode.getPartitionsSplits(partitions, splits);

            // Verify that splits were created
            Assert.assertEquals("Expected 2 splits for 2 base files", 2, splits.size());

        } finally {
            // Clean up static mocks
            reflectionUtilsMock.close();
            bootstrapIndexMock.close();
        }
    }

    @Test
    public void testMorTablePartitionProcessingWithFileSlices(@Injectable SessionVariable sessionVariable,
                                                               @Injectable TupleDescriptor tupleDesc,
                                                               @Injectable HMSExternalTable table,
                                                               @Injectable ExternalCatalog catalog,
                                                               @Injectable HoodieTableMetaClient client) throws Exception {

        new Expectations() {
            {
                client.getBasePathV2();
                result = new Path("/test/base/path");
                minTimes = 0;
            }
        };

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Override to ensure this is a MOR table
        java.lang.reflect.Field isCowOrRoTableField = HudiScanNode.class.getDeclaredField("isCowOrRoTable");
        isCowOrRoTableField.setAccessible(true);
        isCowOrRoTableField.set(scanNode, false);

        // Mock static methods that cause NullPointerException
        MockedStatic<org.apache.hudi.common.util.ReflectionUtils> reflectionUtilsMock =
                Mockito.mockStatic(org.apache.hudi.common.util.ReflectionUtils.class);
        MockedStatic<org.apache.hudi.common.bootstrap.index.BootstrapIndex> bootstrapIndexMock =
                Mockito.mockStatic(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class);

        try {
            // Mock ReflectionUtils to handle null class names
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.getClass(Mockito.any()))
                    .thenReturn(Object.class);
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.loadClass(Mockito.any()))
                    .thenReturn(Object.class);

            // Mock BootstrapIndex to return a mock instance
            bootstrapIndexMock.when(() -> org.apache.hudi.common.bootstrap.index.BootstrapIndex.getBootstrapIndex(Mockito.any()))
                    .thenReturn(Mockito.mock(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class));

            // Create test partition
            HivePartition partition = createMockHivePartition("/test/base/path/partition1", Arrays.asList("2024", "01"));
            List<HivePartition> partitions = Arrays.asList(partition);

            // Create mock file slices with total file sizes
            org.apache.hudi.common.model.FileSlice fileSlice1 = Mockito.mock(org.apache.hudi.common.model.FileSlice.class);
            org.apache.hudi.common.model.FileSlice fileSlice2 = Mockito.mock(org.apache.hudi.common.model.FileSlice.class);

            Mockito.when(fileSlice1.getTotalFileSize()).thenReturn(1500L);
            Mockito.when(fileSlice2.getTotalFileSize()).thenReturn(2500L);

            // Mock base files and log files for file slices
            org.apache.hudi.common.model.HoodieBaseFile baseFile = Mockito.mock(org.apache.hudi.common.model.HoodieBaseFile.class);
            org.apache.hudi.common.util.Option<org.apache.hudi.common.model.HoodieBaseFile> baseFileOption =
                    org.apache.hudi.common.util.Option.of(baseFile);

            Mockito.when(baseFile.getPath()).thenReturn("/test/base/path/partition1/base.parquet");
            Mockito.when(baseFile.getFileSize()).thenReturn(1000L);

            Mockito.when(fileSlice1.getBaseFile()).thenReturn(baseFileOption);
            Mockito.when(fileSlice1.getPartitionPath()).thenReturn("partition1");

            // Mock log files stream
            java.util.stream.Stream<org.apache.hudi.common.model.HoodieLogFile> logFiles =
                    java.util.stream.Stream.empty();
            Mockito.when(fileSlice1.getLogFiles()).thenReturn(logFiles);

            Mockito.when(fileSlice2.getBaseFile()).thenReturn(org.apache.hudi.common.util.Option.empty());
            Mockito.when(fileSlice2.getPartitionPath()).thenReturn("partition1");

            // Create mock log file for second file slice
            org.apache.hudi.common.model.HoodieLogFile logFile = Mockito.mock(org.apache.hudi.common.model.HoodieLogFile.class);
            Mockito.when(logFile.getPath()).thenReturn(new Path("/test/base/path/partition1/log1.log"));
            java.util.stream.Stream<org.apache.hudi.common.model.HoodieLogFile> logFiles2 =
                    java.util.stream.Stream.of(logFile);
            Mockito.when(fileSlice2.getLogFiles()).thenReturn(logFiles2);

            // Mock the HoodieTableFileSystemView constructor and methods using MockUp
            new MockUp<org.apache.hudi.common.table.view.HoodieTableFileSystemView>() {
                @Mock
                public void $init(org.apache.hudi.common.table.HoodieTableMetaClient metaClient, // CHECKSTYLE IGNORE THIS LINE
                                  org.apache.hudi.common.table.timeline.HoodieTimeline timeline,
                                  org.apache.hudi.common.storage.HoodieStorageStrategy storageStrategy) {
                    // Constructor mock - no-op
                }

                @Mock
                public java.util.stream.Stream<org.apache.hudi.common.model.FileSlice> getLatestMergedFileSlicesBeforeOrOn(String partitionPath, String maxCommitTime) {
                    // Return stream of mock file slices for MOR table processing (line 417)
                    return java.util.stream.Stream.of(fileSlice1, fileSlice2);
                }
            };

            // Set storage strategy
            HoodieStorageStrategy storageStrategy = Mockito.mock(HoodieStorageStrategy.class);
            java.lang.reflect.Field storageStrategyField = HudiScanNode.class.getDeclaredField("storageStrategy");
            storageStrategyField.setAccessible(true);
            storageStrategyField.set(scanNode, storageStrategy);

            // Set required fields for HudiSplit creation
            java.lang.reflect.Field inputFormatField = HudiScanNode.class.getDeclaredField("inputFormat");
            inputFormatField.setAccessible(true);
            inputFormatField.set(scanNode, "org.apache.hudi.hadoop.HoodieParquetInputFormat");

            java.lang.reflect.Field serdeLibField = HudiScanNode.class.getDeclaredField("serdeLib");
            serdeLibField.setAccessible(true);
            serdeLibField.set(scanNode, "org.apache.hudi.hive.HoodieHiveSerDe");

            java.lang.reflect.Field basePathField = HudiScanNode.class.getDeclaredField("basePath");
            basePathField.setAccessible(true);
            basePathField.set(scanNode, "/test/base/path");

            java.lang.reflect.Field columnNamesField = HudiScanNode.class.getDeclaredField("columnNames");
            columnNamesField.setAccessible(true);
            columnNamesField.set(scanNode, Arrays.asList("col1", "col2"));

            java.lang.reflect.Field columnTypesField = HudiScanNode.class.getDeclaredField("columnTypes");
            columnTypesField.setAccessible(true);
            columnTypesField.set(scanNode, Arrays.asList("string", "int"));

            java.lang.reflect.Field primaryKeysField = HudiScanNode.class.getDeclaredField("primaryKeys");
            primaryKeysField.setAccessible(true);
            primaryKeysField.set(scanNode, Arrays.asList("col1"));

            // Create test splits list
            List<Split> splits = Collections.synchronizedList(new ArrayList<>());

            // Call the method that should execute line 417
            scanNode.getPartitionsSplits(partitions, splits);

            // Verify that splits were created (should be 2 HudiSplits for 2 file slices)
            Assert.assertEquals("Expected 2 splits for 2 file slices", 2, splits.size());

        } finally {
            // Clean up static mocks
            reflectionUtilsMock.close();
            bootstrapIndexMock.close();
        }
    }

    @Test
    public void testConcurrentPartitionProcessingErrorHandling(@Injectable SessionVariable sessionVariable,
                                                               @Injectable TupleDescriptor tupleDesc,
                                                               @Injectable HMSExternalTable table,
                                                               @Injectable ExternalCatalog catalog,
                                                               @Injectable HoodieTableMetaClient client) throws Exception {

        new Expectations() {
            {
                client.getBasePathV2();
                result = new Path("/test/base/path");
                minTimes = 0;
            }
        };

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Mock static methods that cause NullPointerException
        MockedStatic<org.apache.hudi.common.util.ReflectionUtils> reflectionUtilsMock =
                Mockito.mockStatic(org.apache.hudi.common.util.ReflectionUtils.class);
        MockedStatic<org.apache.hudi.common.bootstrap.index.BootstrapIndex> bootstrapIndexMock =
                Mockito.mockStatic(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class);

        try {
            // Mock ReflectionUtils to handle null class names
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.getClass(Mockito.any()))
                    .thenReturn(Object.class);
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.loadClass(Mockito.any()))
                    .thenReturn(Object.class);

            // Mock BootstrapIndex to return a mock instance
            bootstrapIndexMock.when(() -> org.apache.hudi.common.bootstrap.index.BootstrapIndex.getBootstrapIndex(Mockito.any()))
                    .thenReturn(Mockito.mock(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class));

            // Mock Env to return a custom executor that will cause exceptions
            new MockUp<Env>() {
                @Mock
                public Env getCurrentEnv() {
                    Env env = Mockito.mock(Env.class);
                    org.apache.doris.datasource.ExternalMetaCacheMgr metaCacheMgr =
                            Mockito.mock(org.apache.doris.datasource.ExternalMetaCacheMgr.class);

                    // Create an executor service that throws exceptions
                    java.util.concurrent.ExecutorService faultyExecutor = java.util.concurrent.Executors.newSingleThreadExecutor();

                    Mockito.when(env.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);
                    Mockito.when(metaCacheMgr.getLakehouseGetPartitionSplitExecutor()).thenReturn(faultyExecutor);
                    return env;
                }
            };

            // Create partitions that will cause processing to fail
            HivePartition partition = createMockHivePartition("/test/base/path/partition1", Arrays.asList("2024", "01"));
            List<HivePartition> partitions = Arrays.asList(partition);

            // Mock HoodieTableFileSystemView to throw exception during processing
            new MockUp<org.apache.hudi.common.table.view.HoodieTableFileSystemView>() {
                @Mock
                public void $init(org.apache.hudi.common.table.HoodieTableMetaClient metaClient, // CHECKSTYLE IGNORE THIS LINE
                                  org.apache.hudi.common.table.timeline.HoodieTimeline timeline,
                                  org.apache.hudi.common.storage.HoodieStorageStrategy storageStrategy) {
                    // Constructor mock - no-op
                }

                @Mock
                public java.util.stream.Stream<org.apache.hudi.common.model.HoodieBaseFile> getLatestBaseFilesBeforeOrOn(String partitionPath, String maxCommitTime) {
                    // Throw exception to trigger error handling lines 437-438, then 451, 455
                    throw new RuntimeException("Simulated partition processing error");
                }
            };

            // Set storage strategy
            HoodieStorageStrategy storageStrategy = Mockito.mock(HoodieStorageStrategy.class);
            java.lang.reflect.Field storageStrategyField = HudiScanNode.class.getDeclaredField("storageStrategy");
            storageStrategyField.setAccessible(true);
            storageStrategyField.set(scanNode, storageStrategy);

            // Create test splits list
            List<Split> splits = Collections.synchronizedList(new ArrayList<>());

            // Call the method that should execute error handling lines 427-429, 436, 438, 440
            try {
                scanNode.getPartitionsSplits(partitions, splits);
                Assert.fail("Expected AnalysisException due to partition processing error");
            } catch (AnalysisException e) {
                // Verify that the exception message indicates processing failure
                Assert.assertTrue("Exception message should indicate partition processing failure: " + e.getMessage(),
                        e.getMessage().contains("Failed to process partitions")
                        || e.getMessage().contains("Simulated partition processing error"));
            }

        } finally {
            // Clean up static mocks
            reflectionUtilsMock.close();
            bootstrapIndexMock.close();
        }
    }

    @Test
    public void testInterruptedExceptionDuringPartitionProcessing(@Injectable SessionVariable sessionVariable,
                                                                  @Injectable TupleDescriptor tupleDesc,
                                                                  @Injectable HMSExternalTable table,
                                                                  @Injectable ExternalCatalog catalog,
                                                                  @Injectable HoodieTableMetaClient client) throws Exception {

        new Expectations() {
            {
                client.getBasePathV2();
                result = new Path("/test/base/path");
                minTimes = 0;
            }
        };

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Mock static methods that cause NullPointerException
        MockedStatic<org.apache.hudi.common.util.ReflectionUtils> reflectionUtilsMock =
                Mockito.mockStatic(org.apache.hudi.common.util.ReflectionUtils.class);
        MockedStatic<org.apache.hudi.common.bootstrap.index.BootstrapIndex> bootstrapIndexMock =
                Mockito.mockStatic(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class);

        try {
            // Mock ReflectionUtils to handle null class names
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.getClass(Mockito.any()))
                    .thenReturn(Object.class);
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.loadClass(Mockito.any()))
                    .thenReturn(Object.class);

            // Mock BootstrapIndex to return a mock instance
            bootstrapIndexMock.when(() -> org.apache.hudi.common.bootstrap.index.BootstrapIndex.getBootstrapIndex(Mockito.any()))
                    .thenReturn(Mockito.mock(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class));

            // Mock Env to return a slow executor that allows interruption
            new MockUp<Env>() {
                @Mock
                public Env getCurrentEnv() {
                    Env env = Mockito.mock(Env.class);
                    org.apache.doris.datasource.ExternalMetaCacheMgr metaCacheMgr =
                            Mockito.mock(org.apache.doris.datasource.ExternalMetaCacheMgr.class);

                    // Create an executor service that introduces delay
                    java.util.concurrent.ExecutorService slowExecutor = java.util.concurrent.Executors.newSingleThreadExecutor();

                    Mockito.when(env.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);
                    Mockito.when(metaCacheMgr.getLakehouseGetPartitionSplitExecutor()).thenReturn(slowExecutor);
                    return env;
                }
            };

            // Create partitions
            HivePartition partition = createMockHivePartition("/test/base/path/partition1", Arrays.asList("2024", "01"));
            List<HivePartition> partitions = Arrays.asList(partition);

            // Set storage strategy
            HoodieStorageStrategy storageStrategy = Mockito.mock(HoodieStorageStrategy.class);
            java.lang.reflect.Field storageStrategyField = HudiScanNode.class.getDeclaredField("storageStrategy");
            storageStrategyField.setAccessible(true);
            storageStrategyField.set(scanNode, storageStrategy);

            // Create test splits list
            List<Split> splits = Collections.synchronizedList(new ArrayList<>());

            // Create a thread to interrupt the main processing
            Thread processingThread = Thread.currentThread();
            ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
            scheduler.schedule(() -> {
                processingThread.interrupt();
                scheduler.shutdown();
            }, 50, TimeUnit.MILLISECONDS); // Interrupt after 50ms

            try {
                scanNode.getPartitionsSplits(partitions, splits);
                Assert.fail("Expected AnalysisException due to thread interruption");
            } catch (AnalysisException e) {
                // Verify that the exception message indicates interruption
                Assert.assertTrue("Exception message should indicate interruption: " + e.getMessage(),
                        e.getMessage().contains("Interrupted while processing partitions"));
            } finally {
                // Clear interrupt status
                Thread.interrupted();
                scheduler.shutdown();
            }

        } finally {
            // Clean up static mocks
            reflectionUtilsMock.close();
            bootstrapIndexMock.close();
        }
    }

    @Test
    public void testStartSplitMethodExceptionHandling(@Injectable SessionVariable sessionVariable,
                                                      @Injectable TupleDescriptor tupleDesc,
                                                      @Injectable HMSExternalTable table,
                                                      @Injectable ExternalCatalog catalog,
                                                      @Injectable HoodieTableMetaClient client) throws Exception {

        new Expectations() {
            {
                client.getBasePathV2();
                result = new Path("/test/base/path");
                minTimes = 0;
            }
        };

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Mock static methods that cause NullPointerException
        MockedStatic<org.apache.hudi.common.util.ReflectionUtils> reflectionUtilsMock =
                Mockito.mockStatic(org.apache.hudi.common.util.ReflectionUtils.class);
        MockedStatic<org.apache.hudi.common.bootstrap.index.BootstrapIndex> bootstrapIndexMock =
                Mockito.mockStatic(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class);

        try {
            // Mock ReflectionUtils to handle null class names
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.getClass(Mockito.any()))
                    .thenReturn(Object.class);
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.loadClass(Mockito.any()))
                    .thenReturn(Object.class);

            // Mock BootstrapIndex to return a mock instance
            bootstrapIndexMock.when(() -> org.apache.hudi.common.bootstrap.index.BootstrapIndex.getBootstrapIndex(Mockito.any()))
                    .thenReturn(Mockito.mock(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class));

            // Create partitions for startSplit processing
            HivePartition partition = createMockHivePartition("/test/base/path/partition1", Arrays.asList("2024", "01"));
            List<HivePartition> partitions = Arrays.asList(partition);

            java.lang.reflect.Field prunedPartitionsField = org.apache.doris.datasource.hive.source.HiveScanNode.class.getDeclaredField("prunedPartitions");
            prunedPartitionsField.setAccessible(true);
            prunedPartitionsField.set(scanNode, partitions);

            // Mock HoodieTableFileSystemView to throw exception during processing
            RuntimeException processingException = new RuntimeException("Simulated error during startSplit processing");
            new MockUp<org.apache.hudi.common.table.view.HoodieTableFileSystemView>() {
                @Mock
                public void $init(org.apache.hudi.common.table.HoodieTableMetaClient metaClient, // CHECKSTYLE IGNORE THIS LINE
                                  org.apache.hudi.common.table.timeline.HoodieTimeline timeline,
                                  org.apache.hudi.common.storage.HoodieStorageStrategy storageStrategy) {
                    // Constructor mock - no-op
                }

                @Mock
                public java.util.stream.Stream<org.apache.hudi.common.model.HoodieBaseFile> getLatestBaseFilesBeforeOrOn(String partitionPath, String maxCommitTime) {
                    // Throw exception to trigger async error handling lines 519 (batchException.set)
                    throw processingException;
                }
            };

            // Set storage strategy
            HoodieStorageStrategy storageStrategy = Mockito.mock(HoodieStorageStrategy.class);
            java.lang.reflect.Field storageStrategyField = HudiScanNode.class.getDeclaredField("storageStrategy");
            storageStrategyField.setAccessible(true);
            storageStrategyField.set(scanNode, storageStrategy);

            // Mock split assignment to track exceptions
            org.apache.doris.datasource.SplitAssignment splitAssignment = Mockito.mock(org.apache.doris.datasource.SplitAssignment.class);
            java.lang.reflect.Field splitAssignmentField = org.apache.doris.planner.ScanNode.class.getDeclaredField("splitAssignment");
            splitAssignmentField.setAccessible(true);
            splitAssignmentField.set(scanNode, splitAssignment);

            // Execute startSplit which should trigger lines 513-514 and 519
            scanNode.startSplit(3);

            // Wait a bit for async processing to complete
            Thread.sleep(200);

            // Verify that batchException was set (line 520)
            java.lang.reflect.Field batchExceptionField = HudiScanNode.class.getDeclaredField("batchException");
            batchExceptionField.setAccessible(true);
            java.util.concurrent.atomic.AtomicReference<org.apache.doris.common.UserException> batchException =
                    (java.util.concurrent.atomic.AtomicReference<org.apache.doris.common.UserException>) batchExceptionField.get(scanNode);

            // Verify exception was captured
            Assert.assertNotNull("Expected batchException to be set due to processing error", batchException.get());
            Assert.assertTrue("Exception message should contain simulated error: " + batchException.get().getMessage(),
                    batchException.get().getMessage().contains("Simulated error during startSplit processing"));

            // Verify that setException was called on splitAssignment
            Mockito.verify(splitAssignment, Mockito.atLeast(1)).setException(Mockito.any(org.apache.doris.common.UserException.class));
        } finally {
            // Clean up static mocks
            reflectionUtilsMock.close();
            bootstrapIndexMock.close();
        }
    }

    @Test
    public void testSuccessfulAsyncProcessingInStartSplit(@Injectable SessionVariable sessionVariable,
                                                          @Injectable TupleDescriptor tupleDesc,
                                                          @Injectable HMSExternalTable table,
                                                          @Injectable ExternalCatalog catalog,
                                                          @Injectable HoodieTableMetaClient client) throws Exception {

        new Expectations() {
            {
                client.getBasePathV2();
                result = new Path("/test/base/path");
                minTimes = 0;
            }
        };

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Mock static methods that cause NullPointerException
        MockedStatic<org.apache.hudi.common.util.ReflectionUtils> reflectionUtilsMock =
                Mockito.mockStatic(org.apache.hudi.common.util.ReflectionUtils.class);
        MockedStatic<org.apache.hudi.common.bootstrap.index.BootstrapIndex> bootstrapIndexMock =
                Mockito.mockStatic(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class);

        try {
            // Mock ReflectionUtils to handle null class names
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.getClass(Mockito.any()))
                    .thenReturn(Object.class);
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.loadClass(Mockito.any()))
                    .thenReturn(Object.class);

            // Mock BootstrapIndex to return a mock instance
            bootstrapIndexMock.when(() -> org.apache.hudi.common.bootstrap.index.BootstrapIndex.getBootstrapIndex(Mockito.any()))
                    .thenReturn(Mockito.mock(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class));

            // Create partitions for startSplit processing
            HivePartition partition = createMockHivePartition("/test/base/path/partition1", Arrays.asList("2024", "01"));
            List<HivePartition> partitions = Arrays.asList(partition);

            java.lang.reflect.Field prunedPartitionsField = org.apache.doris.datasource.hive.source.HiveScanNode.class.getDeclaredField("prunedPartitions");
            prunedPartitionsField.setAccessible(true);
            prunedPartitionsField.set(scanNode, partitions);

            // Mock HoodieTableFileSystemView to return successful base files
            org.apache.hudi.common.table.view.HoodieTableFileSystemView fileSystemView =
                    Mockito.mock(org.apache.hudi.common.table.view.HoodieTableFileSystemView.class);

            // Create mock base files for successful processing (lines 513-514 success path)
            org.apache.hudi.common.model.HoodieBaseFile baseFile1 = Mockito.mock(org.apache.hudi.common.model.HoodieBaseFile.class);
            org.apache.hudi.common.model.HoodieBaseFile baseFile2 = Mockito.mock(org.apache.hudi.common.model.HoodieBaseFile.class);

            Mockito.when(baseFile1.getPath()).thenReturn("/test/base/path/partition1/file1.parquet");
            Mockito.when(baseFile1.getFileSize()).thenReturn(1000L);

            Mockito.when(baseFile2.getPath()).thenReturn("/test/base/path/partition1/file2.parquet");
            Mockito.when(baseFile2.getFileSize()).thenReturn(2000L);

            java.util.stream.Stream<org.apache.hudi.common.model.HoodieBaseFile> baseFiles =
                    java.util.stream.Stream.of(baseFile1, baseFile2);

            Mockito.when(fileSystemView.getLatestBaseFilesBeforeOrOn(Mockito.anyString(), Mockito.anyString()))
                    .thenReturn(baseFiles);

            // Set storage strategy
            HoodieStorageStrategy storageStrategy = Mockito.mock(HoodieStorageStrategy.class);
            java.lang.reflect.Field storageStrategyField = HudiScanNode.class.getDeclaredField("storageStrategy");
            storageStrategyField.setAccessible(true);
            storageStrategyField.set(scanNode, storageStrategy);

            // Mock split assignment to track successful completion
            org.apache.doris.datasource.SplitAssignment splitAssignment = Mockito.mock(org.apache.doris.datasource.SplitAssignment.class);
            java.lang.reflect.Field splitAssignmentField = org.apache.doris.planner.ScanNode.class.getDeclaredField("splitAssignment");
            splitAssignmentField.setAccessible(true);
            splitAssignmentField.set(scanNode, splitAssignment);

            // Execute startSplit which should successfully process lines 513-514
            scanNode.startSplit(3);

            // Wait for async processing to complete
            Thread.sleep(300);

            // Verify no exception was set in batchException
            java.lang.reflect.Field batchExceptionField = HudiScanNode.class.getDeclaredField("batchException");
            batchExceptionField.setAccessible(true);
            java.util.concurrent.atomic.AtomicReference<org.apache.doris.common.UserException> batchException =
                    (java.util.concurrent.atomic.AtomicReference<org.apache.doris.common.UserException>) batchExceptionField.get(scanNode);

            // Verify no exception was captured (successful processing)
            Assert.assertNull("Expected no batchException for successful processing", batchException.get());

            // Verify that addToQueue was called with splits (successful processing result)
            Mockito.verify(splitAssignment, Mockito.atLeast(1)).addToQueue(Mockito.anyList());

            // Verify that finishSchedule was called to complete the processing
            Mockito.verify(splitAssignment, Mockito.timeout(500).atLeast(1)).finishSchedule();

            // Verify numSplitsPerPartition was updated (line 515-516)
            java.lang.reflect.Field numSplitsPerPartitionField = HudiScanNode.class.getDeclaredField("numSplitsPerPartition");
            numSplitsPerPartitionField.setAccessible(true);
            java.util.concurrent.atomic.AtomicInteger numSplitsPerPartition =
                    (java.util.concurrent.atomic.AtomicInteger) numSplitsPerPartitionField.get(scanNode);

            // Should be updated to reflect the number of processed files (2 in this case)
            Assert.assertTrue("numSplitsPerPartition should be updated", numSplitsPerPartition.get() >= 2);

        } finally {
            // Clean up static mocks
            reflectionUtilsMock.close();
            bootstrapIndexMock.close();
        }
    }

    @Test
    public void testNonAnalysisExceptionErrorPropagation(@Injectable SessionVariable sessionVariable,
                                                         @Injectable TupleDescriptor tupleDesc,
                                                         @Injectable HMSExternalTable table,
                                                         @Injectable ExternalCatalog catalog,
                                                         @Injectable HoodieTableMetaClient client) throws Exception {

        new Expectations() {
            {
                client.getBasePathV2();
                result = new Path("/test/base/path");
                minTimes = 0;
            }
        };

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Mock static methods that cause NullPointerException
        MockedStatic<org.apache.hudi.common.util.ReflectionUtils> reflectionUtilsMock =
                Mockito.mockStatic(org.apache.hudi.common.util.ReflectionUtils.class);
        MockedStatic<org.apache.hudi.common.bootstrap.index.BootstrapIndex> bootstrapIndexMock =
                Mockito.mockStatic(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class);

        try {
            // Mock ReflectionUtils to handle null class names
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.getClass(Mockito.any()))
                    .thenReturn(Object.class);
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.loadClass(Mockito.any()))
                    .thenReturn(Object.class);

            // Mock BootstrapIndex to return a mock instance
            bootstrapIndexMock.when(() -> org.apache.hudi.common.bootstrap.index.BootstrapIndex.getBootstrapIndex(Mockito.any()))
                    .thenReturn(Mockito.mock(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class));

            // Mock Env to return an executor that will execute tasks
            new MockUp<Env>() {
                @Mock
                public Env getCurrentEnv() {
                    Env env = Mockito.mock(Env.class);
                    org.apache.doris.datasource.ExternalMetaCacheMgr metaCacheMgr =
                            Mockito.mock(org.apache.doris.datasource.ExternalMetaCacheMgr.class);

                    // Create an executor service that executes immediately
                    java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();

                    Mockito.when(env.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);
                    Mockito.when(metaCacheMgr.getLakehouseGetPartitionSplitExecutor()).thenReturn(executor);
                    return env;
                }
            };

            // Create partitions
            HivePartition partition = createMockHivePartition("/test/base/path/partition1", Arrays.asList("2024", "01"));
            List<HivePartition> partitions = Arrays.asList(partition);

            // Mock HoodieTableFileSystemView to throw RuntimeException (not AnalysisException)
            RuntimeException runtimeException = new RuntimeException("Simulated runtime error during file system view operation");
            new MockUp<org.apache.hudi.common.table.view.HoodieTableFileSystemView>() {
                @Mock
                public void $init(org.apache.hudi.common.table.HoodieTableMetaClient metaClient, // CHECKSTYLE IGNORE THIS LINE
                                  org.apache.hudi.common.table.timeline.HoodieTimeline timeline,
                                  org.apache.hudi.common.storage.HoodieStorageStrategy storageStrategy) {
                    // Constructor mock - no-op
                }

                @Mock
                public java.util.stream.Stream<org.apache.hudi.common.model.HoodieBaseFile> getLatestBaseFilesBeforeOrOn(String partitionPath, String maxCommitTime) {
                    // Throw RuntimeException to trigger error handling lines 451, 455
                    throw runtimeException;
                }
            };

            // Set storage strategy
            HoodieStorageStrategy storageStrategy = Mockito.mock(HoodieStorageStrategy.class);
            java.lang.reflect.Field storageStrategyField = HudiScanNode.class.getDeclaredField("storageStrategy");
            storageStrategyField.setAccessible(true);
            storageStrategyField.set(scanNode, storageStrategy);

            // Create test splits list
            List<Split> splits = Collections.synchronizedList(new ArrayList<>());

            // Call the method that should execute error handling lines 451 and 455
            try {
                scanNode.getPartitionsSplits(partitions, splits);
                Assert.fail("Expected AnalysisException due to RuntimeException during partition processing");
            } catch (AnalysisException e) {
                // Verify that the RuntimeException was wrapped in AnalysisException at lines 451, 455
                Assert.assertTrue("Exception message should contain 'Failed to process partitions': " + e.getMessage(),
                        e.getMessage().contains("Failed to process partitions"));

                // Verify the original RuntimeException message is preserved
                Assert.assertTrue("Exception message should contain original error: " + e.getMessage(),
                        e.getMessage().contains("Simulated runtime error")
                        || e.getCause() != null);
            }

        } finally {
            // Clean up static mocks
            reflectionUtilsMock.close();
            bootstrapIndexMock.close();
        }
    }

    @Test
    public void testGetSplits(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc, @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog) {
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                table.isHoodieCowTable();
                result = true;

                catalog.bindBrokerName();
                result = "test";

                table.useHiveSyncPartition();
                result = true;
            }
        };
        HudiScanNode scanNode = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                false, Optional.empty(), Optional.empty(), sessionVariable);
        new MockUp<HudiScanNode>() {
            @Mock
            public List<HivePartition> getPartitions() {
                HivePartition partition1 = new HivePartition("test", "test", false,
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "hdfs://hk-dev01:8121/user/doris/parquet/partition_table/nation=cn/city=beijing",
                        Lists.newArrayList("cn", "beijing"), Maps.newHashMap());
                HivePartition partition2 = new HivePartition("test", "test", false,
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "hdfs://hk-dev01:8121/user/doris/parquet/partition_table/nation=cn/city=shanghai",
                        Lists.newArrayList("cn", "shanghai"), Maps.newHashMap());
                return Lists.newArrayList(partition1, partition2);
            }

            @Mock
            public void getPartitionsSplits(List<HivePartition> partitions, List<Split> splits)
                    throws AnalysisException {
                FileSplit chubaoFileSplit = new FileSplit(new LocationPath(
                        "chubaofs://CHUBAO1101/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5.snappy.orc"),
                        0, 112140970, 112140970, 0, null, Collections.emptyList());
                FileSplit hdfsFileSplit = new FileSplit(new LocationPath(
                        "hdfs://HDFSO1101/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e77f7d8.snappy.orc"),
                        0, 112140970, 112140970, 0, null, Collections.emptyList());
                splits.add(chubaoFileSplit);
                splits.add(hdfsFileSplit);
            }
        };
        try {
            List<Split> splits = scanNode.getSplits(1);
            Assertions.assertFalse(splits.isEmpty());
            Assertions.assertEquals(2, splits.size());
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testGetPartitionsSplitsTimeout(@Injectable SessionVariable sessionVariable,
                                               @Injectable TupleDescriptor tupleDesc,
                                               @Injectable HMSExternalTable table,
                                               @Injectable ExternalCatalog catalog,
                                               @Injectable HoodieTableMetaClient client) throws Exception {
        // Test lines 456-460: Timeout handling during partition processing

        new Expectations() {
            {
                client.getBasePathV2();
                result = new Path("/test/base/path");
                minTimes = 0;
            }
        };

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Mock static methods that cause NullPointerException
        MockedStatic<org.apache.hudi.common.util.ReflectionUtils> reflectionUtilsMock =
                Mockito.mockStatic(org.apache.hudi.common.util.ReflectionUtils.class);
        MockedStatic<org.apache.hudi.common.bootstrap.index.BootstrapIndex> bootstrapIndexMock =
                Mockito.mockStatic(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class);

        // Save original config value
        int originalTimeout = Config.lakehouse_get_split_max_second;
        Config.lakehouse_get_split_max_second = 1; // Set to 1 second for testing

        try {
            // Mock ReflectionUtils to handle null class names
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.getClass(Mockito.any()))
                    .thenReturn(Object.class);
            reflectionUtilsMock.when(() -> org.apache.hudi.common.util.ReflectionUtils.loadClass(Mockito.any()))
                    .thenReturn(Object.class);

            // Mock BootstrapIndex to return a mock instance
            bootstrapIndexMock.when(() -> org.apache.hudi.common.bootstrap.index.BootstrapIndex.getBootstrapIndex(Mockito.any()))
                    .thenReturn(Mockito.mock(org.apache.hudi.common.bootstrap.index.BootstrapIndex.class));

            // Mock Env to return a slow executor that will cause timeout
            new MockUp<Env>() {
                @Mock
                public Env getCurrentEnv() {
                    Env env = Mockito.mock(Env.class);
                    org.apache.doris.datasource.ExternalMetaCacheMgr metaCacheMgr =
                            Mockito.mock(org.apache.doris.datasource.ExternalMetaCacheMgr.class);

                    // Create an executor that will delay processing
                    java.util.concurrent.ExecutorService slowExecutor = java.util.concurrent.Executors.newSingleThreadExecutor();

                    Mockito.when(env.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);
                    Mockito.when(metaCacheMgr.getLakehouseGetPartitionSplitExecutor()).thenReturn(slowExecutor);
                    return env;
                }
            };

            // Create partitions
            HivePartition partition = createMockHivePartition("/test/base/path/partition1", Arrays.asList("2024", "01"));
            List<HivePartition> partitions = Arrays.asList(partition);

            // Mock HoodieTableFileSystemView to delay processing and cause timeout
            new MockUp<org.apache.hudi.common.table.view.HoodieTableFileSystemView>() {
                @Mock
                public void $init(org.apache.hudi.common.table.HoodieTableMetaClient metaClient, // CHECKSTYLE IGNORE THIS LINE
                                  org.apache.hudi.common.table.timeline.HoodieTimeline timeline,
                                  org.apache.hudi.common.storage.HoodieStorageStrategy storageStrategy) {
                    // Constructor mock - no-op
                }

                @Mock
                public java.util.stream.Stream<org.apache.hudi.common.model.HoodieBaseFile> getLatestBaseFilesBeforeOrOn(String partitionPath, String maxCommitTime) {
                    // Delay to cause timeout (lines 456-460)
                    try {
                        Thread.sleep(5000); // Sleep longer than the 1 second timeout
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return java.util.stream.Stream.empty();
                }
            };

            // Set storage strategy
            HoodieStorageStrategy storageStrategy = Mockito.mock(HoodieStorageStrategy.class);
            java.lang.reflect.Field storageStrategyField = HudiScanNode.class.getDeclaredField("storageStrategy");
            storageStrategyField.setAccessible(true);
            storageStrategyField.set(scanNode, storageStrategy);

            // Mock table for error message
            new Expectations() {
                {
                    table.getDbName();
                    result = "testDb";

                    table.getName();
                    result = "testTable";
                }
            };

            // Create test splits list
            List<Split> splits = Collections.synchronizedList(new ArrayList<>());

            // Call the method that should trigger timeout (lines 456-460)
            try {
                scanNode.getPartitionsSplits(partitions, splits);
                Assert.fail("Expected AnalysisException due to timeout");
            } catch (AnalysisException e) {
                // Verify that the exception message indicates timeout (lines 459-460)
                Assert.assertTrue("Exception message should mention timeout: " + e.getMessage(),
                        e.getMessage().contains("Timeout while processing partitions")
                        || e.getMessage().contains("timeout"));
                Assert.assertTrue("Exception message should contain table name: " + e.getMessage(),
                        e.getMessage().contains("testDb.testTable")
                        || e.getMessage().contains("testTable"));
            }

        } finally {
            Config.lakehouse_get_split_max_second = originalTimeout;
            // Clean up static mocks
            reflectionUtilsMock.close();
            bootstrapIndexMock.close();
        }
    }
}
