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
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.planner.ListPartitionPrunerV2;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.BDPAuthContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TFileRangeDesc;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class HudiScanNodeTest {

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

            java.lang.reflect.Field partitionInitField = HudiScanNode.class.getDeclaredField("partitionInit");
            partitionInitField.setAccessible(true);
            partitionInitField.set(scanNode, true);

            java.lang.reflect.Field prunedPartitionsField = HudiScanNode.class.getDeclaredField("prunedPartitions");
            prunedPartitionsField.setAccessible(true);
            prunedPartitionsField.set(scanNode, new ArrayList<HivePartition>());
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

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Set a very low max file size limit
        long oldMaxFileSize = Config.max_selected_total_file_size_for_lakehouse_table;
        Config.max_selected_total_file_size_for_lakehouse_table = 1000; // 1KB for testing

        try {
            // Create partitions
            List<HivePartition> partitions = Arrays.asList(
                    createMockHivePartition("/test/base/path/partition1", Arrays.asList("2024", "01"))
            );

            java.lang.reflect.Field prunedPartitionsField = HudiScanNode.class.getDeclaredField("prunedPartitions");
            prunedPartitionsField.setAccessible(true);
            prunedPartitionsField.set(scanNode, partitions);

            // Mock BDPAuthContext
            new MockUp<BDPAuthContext>() {
                @Mock
                public BDPAuthContext get() {
                    BDPAuthContext context = Mockito.mock(BDPAuthContext.class);
                    Mockito.when(context.getHadoopUserName()).thenReturn("testUser");
                    Mockito.when(context.getUserToken()).thenReturn("testToken");
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

            // Execute and expect exception
            Assert.assertThrows(AnalysisException.class, () -> scanNode.getSplits(3));
        } finally {
            Config.max_selected_total_file_size_for_lakehouse_table = oldMaxFileSize;
        }
    }

    @Test
    public void testGetPartitionMetadata(@Injectable SessionVariable sessionVariable,
                                        @Injectable TupleDescriptor tupleDesc,
                                        @Injectable HMSExternalTable table,
                                        @Injectable ExternalCatalog catalog,
                                        @Injectable HoodieTableMetaClient client) throws Exception {

        new Expectations() {
            {
                client.getBasePathV2();
                result = new Path("/test/base/path");
            }
        };

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Set up storage strategy
        HoodieStorageStrategy storageStrategy = Mockito.mock(HoodieStorageStrategy.class);
        java.lang.reflect.Field storageStrategyField = HudiScanNode.class.getDeclaredField("storageStrategy");
        storageStrategyField.setAccessible(true);
        storageStrategyField.set(scanNode, storageStrategy);

        // Mock BDPAuthContext
        new MockUp<BDPAuthContext>() {
            @Mock
            public BDPAuthContext get() {
                BDPAuthContext context = Mockito.mock(BDPAuthContext.class);
                Mockito.when(context.getHadoopUserName()).thenReturn("testUser");
                Mockito.when(context.getUserToken()).thenReturn("testToken");
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
        Method getPartitionMetadataMethod = HudiScanNode.class.getDeclaredMethod("getPartitionMetadata", HivePartition.class);
        getPartitionMetadataMethod.setAccessible(true);
        Object metadata = getPartitionMetadataMethod.invoke(scanNode, partition);

        // Verify the metadata
        Assert.assertNotNull(metadata);

        // Access PartitionMetadata fields via reflection
        Class<?> partitionMetadataClass = metadata.getClass();
        java.lang.reflect.Field totalSizeField = partitionMetadataClass.getDeclaredField("totalSize");
        totalSizeField.setAccessible(true);
        long totalSize = (long) totalSizeField.get(metadata);
        Assert.assertEquals(3000L, totalSize); // 1000 + 2000

        java.lang.reflect.Field statusesField = partitionMetadataClass.getDeclaredField("statuses");
        statusesField.setAccessible(true);
        List<FileStatus> statuses = (List<FileStatus>) statusesField.get(metadata);
        Assert.assertEquals(2, statuses.size());
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
        // Mockito.verify(incrementalRelation).collectSplits();
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

        java.lang.reflect.Field prunedPartitionsField = HudiScanNode.class.getDeclaredField("prunedPartitions");
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

        // Create test partitions
        List<HivePartition> partitions = Arrays.asList(
                createMockHivePartition("/test/base/path/partition1", Arrays.asList("2024", "01"))
        );

        java.lang.reflect.Field prunedPartitionsField = HudiScanNode.class.getDeclaredField("prunedPartitions");
        prunedPartitionsField.setAccessible(true);
        prunedPartitionsField.set(scanNode, partitions);

        HoodieStorageStrategy storageStrategy = Mockito.mock(HoodieStorageStrategy.class);
        java.lang.reflect.Field storageStrategyField = HudiScanNode.class.getDeclaredField("storageStrategy");
        storageStrategyField.setAccessible(true);
        storageStrategyField.set(scanNode, storageStrategy);

        // Mock BDPAuthContext
        new MockUp<BDPAuthContext>() {
            @Mock
            public BDPAuthContext get() {
                BDPAuthContext context = Mockito.mock(BDPAuthContext.class);
                Mockito.when(context.getHadoopUserName()).thenReturn("testUser");
                Mockito.when(context.getUserToken()).thenReturn("testToken");
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
    }

    @Test
    public void testEmptyPartitions(@Injectable SessionVariable sessionVariable,
                                    @Injectable TupleDescriptor tupleDesc,
                                    @Injectable HMSExternalTable table,
                                    @Injectable ExternalCatalog catalog,
                                    @Injectable HoodieTableMetaClient client) throws Exception {

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Set empty partitions
        List<HivePartition> partitions = new ArrayList<>();
        java.lang.reflect.Field prunedPartitionsField = HudiScanNode.class.getDeclaredField("prunedPartitions");
        prunedPartitionsField.setAccessible(true);
        prunedPartitionsField.set(scanNode, partitions);

        // Execute
        List<Split> splits = scanNode.getSplits(3);

        // Verify
        Assert.assertNotNull(splits);
        Assert.assertEquals(0, splits.size());
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
            java.lang.reflect.Field partitionInitField = HudiScanNode.class.getDeclaredField("partitionInit");
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

        java.lang.reflect.Field prunedPartitionsField = HudiScanNode.class.getDeclaredField("prunedPartitions");
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
}
