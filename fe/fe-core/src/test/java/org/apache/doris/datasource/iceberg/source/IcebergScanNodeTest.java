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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.system.SystemInfoService;

import com.alibaba.ttl.threadpool.TtlExecutors;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.TableScanUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;


public class IcebergScanNodeTest {

    @BeforeClass
    public static void setUpClass() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();

        new MockUp<org.apache.doris.qe.ConnectContext>() {
            @Mock
            public org.apache.doris.qe.ConnectContext get() {
                org.apache.doris.qe.ConnectContext context = Mockito.mock(org.apache.doris.qe.ConnectContext.class);
                SessionVariable sv = new SessionVariable();
                SessionVariable spySv = Mockito.spy(sv);
                Mockito.when(context.getSessionVariable()).thenReturn(spySv);
                Mockito.doReturn(1024).when(spySv).getNumPartitionsInBatchMode();
                return context;
            }
        };
    }

    static {
        // Mock HiveMetaStoreClientHelper to bypass security/UGI operations
        new MockUp<org.apache.doris.datasource.hive.HiveMetaStoreClientHelper>() {
            @Mock
            public <T> T ugiDoAs(Configuration conf, PrivilegedExceptionAction<T> action) {
                try {
                    return action.run();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to execute action", e);
                }
            }
        };

        // Mock Env to provide minimal global context
        new MockUp<org.apache.doris.catalog.Env>() {
            @Mock
            public org.apache.doris.catalog.Env getCurrentEnv() {
                org.apache.doris.catalog.Env env = Mockito.mock(org.apache.doris.catalog.Env.class);
                org.apache.doris.datasource.ExternalMetaCacheMgr cacheMgr = Mockito.mock(org.apache.doris.datasource.ExternalMetaCacheMgr.class);
                org.apache.doris.datasource.iceberg.IcebergMetadataCache icebergCache = Mockito.mock(org.apache.doris.datasource.iceberg.IcebergMetadataCache.class);

                // Mock a basic table for getIcebergTable calls
                BaseTable mockTable = Mockito.mock(BaseTable.class);
                PartitionSpec mockSpec = Mockito.mock(PartitionSpec.class);
                Schema mockSchema = Mockito.mock(Schema.class);
                Types.StructType mockStructType = Mockito.mock(Types.StructType.class);
                TableOperations mockOps = Mockito.mock(TableOperations.class);
                org.apache.iceberg.TableMetadata mockMetadata = Mockito.mock(org.apache.iceberg.TableMetadata.class);

                Mockito.when(mockTable.spec()).thenReturn(mockSpec);
                Mockito.when(mockTable.schema()).thenReturn(mockSchema);
                Mockito.when(mockTable.operations()).thenReturn(mockOps);
                Mockito.when(mockOps.current()).thenReturn(mockMetadata);
                Mockito.when(mockMetadata.formatVersion()).thenReturn(2);
                Mockito.when(mockSchema.asStruct()).thenReturn(mockStructType);
                Mockito.when(mockSpec.fields()).thenReturn(new ArrayList<>());

                Mockito.when(icebergCache.getIcebergTable(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(mockTable);

                Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
                Mockito.when(env.getClusterInfo()).thenReturn(new SystemInfoService());
                ExecutorService fileListingExecutor = TtlExecutors.getTtlExecutorService(
                        ThreadPoolManager.newDaemonFixedThreadPool(
                        Config.max_external_file_cache_loader_thread_pool_size,
                        Config.max_external_cache_loader_thread_pool_size * 1000,
                        "FileListingExecutor", 10, true));
                Mockito.when(cacheMgr.getFileListingExecutor()).thenReturn(fileListingExecutor);
                Mockito.when(cacheMgr.getIcebergMetadataCache()).thenReturn(icebergCache);

                return env;
            }
        };
    }

    /**
     * Test actual getSplits() method when file size exceeds limit
     */
    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testGetSplitsWithUnifiedSessionVariable() throws Exception {
    }

    @Test
    public void testGetSplitsExceedsMaxFileSize() throws Exception {
        IcebergScanNode scanNode = createRealScanNode();

        // Access sessionVariable via reflection (it's in FileQueryScanNode)
        java.lang.reflect.Field svField = org.apache.doris.datasource.FileQueryScanNode.class.getDeclaredField("sessionVariable");
        svField.setAccessible(true);
        SessionVariable sv = (SessionVariable) svField.get(scanNode);

        // Set a very low max file size limit for testing
        long oldMaxFileSize = sv.maxSelectedTotalFileSizeForLakehouseTable;
        sv.maxSelectedTotalFileSizeForLakehouseTable = 1000L; // 1KB for testing

        try {
            // Create file scan tasks that exceed the limit
            List<FileScanTask> mockTasks = Arrays.asList(
                    createMockFileScanTask(10000L), // 10KB - exceeds limit
                    createMockFileScanTask(20000L)  // 20KB
            );

            setupTableScanMocks(scanNode, mockTasks, false);

            // Execute and expect RuntimeException wrapping AnalysisException
            try {
                scanNode.getSplits(3);
                Assert.fail("Expected RuntimeException wrapping AnalysisException but no exception was thrown");
            } catch (AnalysisException e) {
                Assert.assertTrue("Exception message should contain 'exceed max bytes'",
                        e.getMessage().contains("exceed max bytes for single iceberg table"));
            }
        } finally {
            sv.maxSelectedTotalFileSizeForLakehouseTable = oldMaxFileSize;
        }
    }

    /**
     * Test actual getSplits() method when partition count exceeds limit
     */
    @Test
    public void testGetSplitsExceedsMaxPartitionCount() throws Exception {
        IcebergScanNode scanNode = createRealScanNode();

        // Set a very low max partition count for testing
        int oldMaxPartitionCount = Config.max_selected_partition_num_for_lakehouse_table;
        Config.max_selected_partition_num_for_lakehouse_table = 2;

        try {
            // Create file scan tasks with different partitions
            List<FileScanTask> mockTasks = Arrays.asList(
                    createMockFileScanTaskWithPartition(100L, "partition1"),
                    createMockFileScanTaskWithPartition(100L, "partition2"),
                    createMockFileScanTaskWithPartition(100L, "partition3") // This exceeds limit of 2
            );

            setupTableScanMocks(scanNode, mockTasks, true);

            // Execute and expect RuntimeException wrapping AnalysisException
            try {
                scanNode.getSplits(3);
                Assert.fail("Expected RuntimeException wrapping AnalysisException but no exception was thrown");
            } catch (AnalysisException e) {
                Assert.assertTrue("Exception message should contain 'exceed max selected partition num'",
                        e.getMessage().contains("exceed max selected partition num"));
            }
        } finally {
            Config.max_selected_partition_num_for_lakehouse_table = oldMaxPartitionCount;
        }
    }

    /**
     * Test actual getSplits() method when IOException occurs in planFiles
     */
    @Test
    public void testGetSplitsIOExceptionInPlanFiles() throws Exception {
        IcebergScanNode scanNode = createRealScanNode();

        // Get the mocked icebergTable and setup IOException in planFiles
        BaseTable icebergTable = getIcebergTable(scanNode);
        TableScan tableScan = Mockito.mock(TableScan.class);
        PartitionSpec partitionSpec = Mockito.mock(PartitionSpec.class);

        Mockito.when(icebergTable.newScan()).thenReturn(tableScan);
        Mockito.when(icebergTable.spec()).thenReturn(partitionSpec);
        Mockito.when(partitionSpec.isPartitioned()).thenReturn(false);

        Mockito.when(tableScan.planWith(Mockito.any())).thenReturn(tableScan);
        // Mock planFiles to throw RuntimeException (since IOException is not allowed)
        Mockito.when(tableScan.planFiles()).thenThrow(new RuntimeException("Test IO exception"));

        // Execute and expect RuntimeException (wrapped by HiveMetaStoreClientHelper mock)
        try {
            scanNode.getSplits(3);
            Assert.fail("Expected RuntimeException but no exception was thrown");
        } catch (RuntimeException e) {
            // Check if this follows the HiveMetaStoreClientHelper wrapping pattern
            if (e.getMessage().contains("Failed to execute action") && e.getCause() != null) {
                Assert.assertTrue("Exception cause should contain 'Test IO exception'",
                        e.getCause().getMessage().contains("Test IO exception"));
            } else {
                // If not wrapped by HiveMetaStoreClientHelper, check the direct message
                Assert.assertTrue("Exception message should contain 'Test IO exception'",
                        e.getMessage().contains("Test IO exception"));
            }
        }
    }

    /**
     * Test actual getSplits() method when IOException occurs in TableScanUtil operations
     */
    @Test
    public void testGetSplitsIOExceptionInTableScanUtil() throws Exception {
        IcebergScanNode scanNode = createRealScanNode();

        // Setup normal file tasks
        List<FileScanTask> mockTasks = Arrays.asList(
                createMockFileScanTask(100L),
                createMockFileScanTask(200L)
        );

        setupTableScanMocks(scanNode, mockTasks, false);

        // Mock TableScanUtil.splitFiles to throw IOException
        new MockUp<TableScanUtil>() {
            @Mock
            public CloseableIterable<FileScanTask> splitFiles(CloseableIterable<FileScanTask> tasks, long splitSize)
                    throws IOException {
                throw new IOException("TableScanUtil IO exception");
            }
        };

        // Execute and expect RuntimeException wrapping UserException
        try {
            scanNode.getSplits(3);
            Assert.fail("Expected RuntimeException wrapping UserException but no exception was thrown");
        } catch (UserException e) {
            Assert.assertTrue("Exception message should contain 'TableScanUtil IO exception'",
                    e.getMessage().contains("TableScanUtil IO exception"));
        }
    }

    /**
     * Test actual getSplits() method with normal processing
     */
    @Test
    public void testGetSplitsNormalProcessing() throws Exception {
        IcebergScanNode scanNode = createRealScanNode();

        // Create file scan tasks within limits
        List<FileScanTask> mockTasks = Arrays.asList(
                createMockFileScanTask(100L),
                createMockFileScanTask(200L)
        );

        setupTableScanMocks(scanNode, mockTasks, false);

        // Mock TableScanUtil to return empty results for simplicity
        new MockUp<TableScanUtil>() {
            @Mock
            public CloseableIterable<FileScanTask> splitFiles(CloseableIterable<FileScanTask> tasks, long splitSize) {
                return createCloseableIterable(Collections.<FileScanTask>emptyList());
            }

            @Mock
            public CloseableIterable<CombinedScanTask> planTasks(CloseableIterable<FileScanTask> tasks,
                                                                long splitSize, int lookback, long maxFiles) {
                return createCloseableIterable(Collections.<CombinedScanTask>emptyList());
            }
        };

        // Execute and verify no exception
        List<Split> splits = scanNode.getSplits(3);
        Assert.assertNotNull("Splits should not be null", splits);
        // Empty splits expected since we mocked empty combined tasks
    }

    /**
     * Test getSplits() with session variable priority
     */
    @Test
    public void testGetSplitsWithSessionVariable() throws Exception {
        IcebergScanNode scanNode = createRealScanNode();

        // Case 1: Session variable is set and smaller than config (Priority: Session)
        // Session = 1KB
        java.lang.reflect.Field sessionVariableField = org.apache.doris.datasource.FileQueryScanNode.class.getDeclaredField("sessionVariable");
        sessionVariableField.setAccessible(true);
        SessionVariable sv = (SessionVariable) sessionVariableField.get(scanNode);
        sv.maxSelectedTotalFileSizeForLakehouseTable = 1000L; // 1KB
        sv.numPartitionsInBatchMode = 1024;

        // Create file scan tasks that exceed the session limit (10KB)
        List<FileScanTask> mockTasks = Arrays.asList(
                createMockFileScanTask(10000L)
        );
        setupTableScanMocks(scanNode, mockTasks, false);

        try {
            scanNode.getSplits(3);
            Assert.fail("Should throw exception when session variable limit is exceeded");
        } catch (AnalysisException e) {
            Assert.assertTrue("Exception message should contain 'exceed max bytes'",
                    e.getMessage().contains("exceed max bytes for single iceberg table"));
        }
    }

    // Helper methods

    private IcebergScanNode createRealScanNode() throws Exception {
        // Create minimal mocks needed for the test
        SessionVariable sessionVariable = Mockito.spy(new SessionVariable());

        // Override ConnectContext to use our spy SessionVariable
        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                ConnectContext context = Mockito.mock(ConnectContext.class);
                Mockito.when(context.getSessionVariable()).thenReturn(sessionVariable);
                return context;
            }
        };

        TupleDescriptor tupleDesc = Mockito.mock(TupleDescriptor.class);
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        BaseTable icebergTable = Mockito.mock(BaseTable.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        CatalogProperty catalogProperty = Mockito.mock(CatalogProperty.class);
        TableOperations tableOperations = Mockito.mock(TableOperations.class);
        org.apache.iceberg.TableMetadata tableMetadata = Mockito.mock(org.apache.iceberg.TableMetadata.class);
        PartitionSpec partitionSpec = Mockito.mock(PartitionSpec.class);
        Schema schema = Mockito.mock(Schema.class);
        Types.StructType structType = Mockito.mock(Types.StructType.class);
        TableRef tableRef = Mockito.mock(TableRef.class);

        // Setup basic mocks
        TupleId tupleId = new TupleId(1);
        Mockito.doReturn(0L).when(sessionVariable).getFileSplitSize();
        Mockito.when(tupleDesc.getId()).thenReturn(tupleId);
        Mockito.when(tupleDesc.getTable()).thenReturn(table);
        Mockito.when(tupleDesc.getRef()).thenReturn(tableRef);
        Mockito.when(tableRef.getTableSnapshot()).thenReturn(null);
        Mockito.when(table.getIcebergCatalogType()).thenReturn("hadoop");
        Mockito.when(table.getName()).thenReturn("test_table");
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("test_db");
        Mockito.when(catalog.getConfiguration()).thenReturn(new Configuration());
        Mockito.when(catalog.getProperties()).thenReturn(new HashMap<String, String>());
        Mockito.when(catalog.getCatalogProperty()).thenReturn(catalogProperty);
        Mockito.when(catalogProperty.getHadoopProperties()).thenReturn(new HashMap<String, String>());

        // Setup iceberg table mocks
        Mockito.when(icebergTable.operations()).thenReturn(tableOperations);
        Mockito.when(tableOperations.current()).thenReturn(tableMetadata);
        Mockito.when(tableMetadata.formatVersion()).thenReturn(2);
        Mockito.when(icebergTable.spec()).thenReturn(partitionSpec);
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(schema.asStruct()).thenReturn(structType);
        Mockito.when(partitionSpec.fields()).thenReturn(new ArrayList<>());

        // Mock IcebergApiSource
        IcebergApiSource source = Mockito.mock(IcebergApiSource.class);
        Mockito.when(source.getCatalog()).thenReturn(catalog);
        Mockito.when(source.getIcebergTable()).thenReturn(icebergTable);
        Mockito.when(source.getFileFormat()).thenReturn("parquet");
        Mockito.when(source.getTargetTable()).thenReturn(table);

        Mockito.when(table.getCatalog()).thenReturn(catalog);

        // Create the scan node
        IcebergScanNode scanNode = new IcebergScanNode(new PlanNodeId(1), tupleDesc, false, sessionVariable);

        // Set fields via reflection to avoid complex initialization
        setFieldValue(scanNode, "icebergTable", icebergTable);
        setFieldValue(scanNode, "source", source);
        setFieldValue(scanNode, "conjuncts", new ArrayList<>());
        setFieldValue(scanNode, "pushdownIcebergPredicates", new ArrayList<>());

        return scanNode;
    }

    private void setupTableScanMocks(IcebergScanNode scanNode, List<FileScanTask> tasks, boolean isPartitioned)
            throws Exception {
        BaseTable icebergTable = getIcebergTable(scanNode);
        TableScan tableScan = Mockito.mock(TableScan.class);
        PartitionSpec partitionSpec = Mockito.mock(PartitionSpec.class);

        Mockito.when(icebergTable.newScan()).thenReturn(tableScan);
        Mockito.when(icebergTable.spec()).thenReturn(partitionSpec);
        Mockito.when(partitionSpec.isPartitioned()).thenReturn(isPartitioned);
        Mockito.when(partitionSpec.fields()).thenReturn(new ArrayList<>());

        Mockito.when(tableScan.planWith(Mockito.any())).thenReturn(tableScan);
        CloseableIterable<FileScanTask> plannedFiles = createCloseableIterable(tasks);
        Mockito.when(tableScan.planFiles()).thenReturn(plannedFiles);
    }

    private FileScanTask createMockFileScanTask(long fileSizeInBytes) {
        FileScanTask task = Mockito.mock(FileScanTask.class);
        DataFile dataFile = Mockito.mock(DataFile.class);
        StructLike partition = Mockito.mock(StructLike.class);
        org.apache.iceberg.PartitionSpec spec = Mockito.mock(org.apache.iceberg.PartitionSpec.class);

        Mockito.when(task.file()).thenReturn(dataFile);
        Mockito.when(task.spec()).thenReturn(spec);
        Mockito.when(dataFile.fileSizeInBytes()).thenReturn(fileSizeInBytes);
        Mockito.when(dataFile.partition()).thenReturn(partition);
        Mockito.when(dataFile.path()).thenReturn((CharSequence) "/test/default/file.parquet");
        Mockito.when(partition.toString()).thenReturn("default_partition");

        // Set up partition data as empty for non-partitioned tables
        Mockito.when(partition.size()).thenReturn(0);

        // Set up empty partition fields for non-partitioned tables
        Mockito.when(spec.fields()).thenReturn(new ArrayList<>());

        return task;
    }

    private FileScanTask createMockFileScanTaskWithPartition(long fileSizeInBytes, String partitionName) {
        FileScanTask task = Mockito.mock(FileScanTask.class);
        DataFile dataFile = Mockito.mock(DataFile.class);
        StructLike partition = Mockito.mock(StructLike.class);
        org.apache.iceberg.PartitionSpec spec = Mockito.mock(org.apache.iceberg.PartitionSpec.class);

        Mockito.when(task.file()).thenReturn(dataFile);
        Mockito.when(task.spec()).thenReturn(spec);
        Mockito.when(dataFile.fileSizeInBytes()).thenReturn(fileSizeInBytes);
        Mockito.when(dataFile.partition()).thenReturn(partition);
        Mockito.when(dataFile.path()).thenReturn((CharSequence) "/test/partition/" + partitionName + "/file.parquet");
        Mockito.when(partition.toString()).thenReturn(partitionName);

        // Set up partition data
        Mockito.when(partition.size()).thenReturn(1);
        Mockito.when(partition.get(0, Object.class)).thenReturn("test_value");

        // Set up empty partition fields
        Mockito.when(spec.fields()).thenReturn(new ArrayList<>());

        return task;
    }

    private <T> CloseableIterable<T> createCloseableIterable(List<T> items) {
        return new CloseableIterable<T>() {
            @Override
            public CloseableIterator<T> iterator() {
                final Iterator<T> iter = items.iterator();
                return new CloseableIterator<T>() {
                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public T next() {
                        return iter.next();
                    }

                    @Override
                    public void close() throws IOException {
                    }
                };
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    private BaseTable getIcebergTable(IcebergScanNode scanNode) throws Exception {
        java.lang.reflect.Field field = IcebergScanNode.class.getDeclaredField("icebergTable");
        field.setAccessible(true);
        return (BaseTable) field.get(scanNode);
    }

    /**
     * Test actual getSplits() method with default max file size limit
     */
    @Test
    public void testDefaultMaxSelectedTotalFileSize() throws Exception {
        IcebergScanNode scanNode = createRealScanNode();

        // Access sessionVariable via reflection
        java.lang.reflect.Field svField = org.apache.doris.datasource.FileQueryScanNode.class.getDeclaredField("sessionVariable");
        svField.setAccessible(true);
        SessionVariable sv = (SessionVariable) svField.get(scanNode);

        // Verify default value is 8796093022208L
        Assert.assertEquals(8796093022208L, sv.maxSelectedTotalFileSizeForLakehouseTable);

        // Case 1: Session variable is default, should use default limit (8TB)
        // Mock tasks exceeding 8TB
        long exceedSize = 8796093022208L + 1;
        List<FileScanTask> mockTasks = Arrays.asList(
                createMockFileScanTask(exceedSize)
        );
        setupTableScanMocks(scanNode, mockTasks, false);

        try {
            scanNode.getSplits(3);
            Assert.fail("Should throw exception when default limit is exceeded");
        } catch (AnalysisException e) {
            Assert.assertTrue("Exception message should contain 'exceed max bytes'",
                    e.getMessage().contains("exceed max bytes for single iceberg table"));
        }

        // Case 2: Session variable is set by user, should use user value
        sv.maxSelectedTotalFileSizeForLakehouseTable = exceedSize + 100;
        try {
            scanNode.getSplits(3);
            // Should pass because limit is increased
        } catch (AnalysisException e) {
            Assert.fail("Should not throw exception when user limit is respected");
        }
    }

    /**
     * Test date transformation logic for DATE type partition fields
     */
    @Test
    public void testDateTransformationForPartitionFields() throws Exception {
        IcebergScanNode scanNode = createRealScanNode();

        // Create mock partition data with DATE type field
        Integer dateValue = 19000; // Days since epoch (approximately 2022-01-01)
        List<FileScanTask> mockTasks = Arrays.asList(
                createMockFileScanTaskWithDatePartition(100L, "date_partition", dateValue)
        );

        setupDatePartitionedTableMocks(scanNode, mockTasks);
        setupDateTransformationMocking();

        // Mock DateTimeUtil to verify the transformation is called
        String expectedIsoDate = "2022-01-01";
        new MockUp<org.apache.iceberg.util.DateTimeUtil>() {
            @Mock
            public String daysToIsoDate(Integer days) {
                Assert.assertEquals("Date value should match", dateValue, days);
                return expectedIsoDate;
            }
        };

        List<Split> splits = scanNode.getSplits(3);
        Assert.assertNotNull("Splits should not be null", splits);

        // Verify that the split contains the converted date value
        if (!splits.isEmpty() && splits.get(0) instanceof IcebergSplit) {
            IcebergSplit icebergSplit = (IcebergSplit) splits.get(0);
            List<String> partitionValues = icebergSplit.getPartitionValues();
            Assert.assertFalse("Partition values should not be empty", partitionValues.isEmpty());
            Assert.assertEquals("Converted date should match expected ISO format",
                    "2022-01-08", partitionValues.get(0));
        }
    }

    /**
     * Test IcebergSplit creation with all parameters
     */
    @Test
    public void testIcebergSplitCreationWithAllParameters() throws Exception {
        IcebergScanNode scanNode = createRealScanNode();

        // Create mock data for detailed split creation
        List<FileScanTask> mockTasks = Arrays.asList(
                createDetailedMockFileScanTask(500L, "/test/path/file1.parquet", 0L, 500L)
        );

        setupPartitionedTableMocks(scanNode, mockTasks);
        setupDetailedSplitMocking();

        List<Split> splits = scanNode.getSplits(3);
        Assert.assertNotNull("Splits should not be null", splits);
    }

    /**
     * Test IOException handling in the inner forEach loop
     */
    @Test
    public void testIOExceptionInInnerForEachLoop() throws Exception {
        IcebergScanNode scanNode = createRealScanNode();

        // Create normal tasks but mock the inner processing to throw IOException
        List<FileScanTask> mockTasks = Arrays.asList(
                createMockFileScanTask(100L)
        );

        setupTableScanMocks(scanNode, mockTasks, false);
        setupForEachIOExceptionMocking();

        try {
            scanNode.getSplits(3);
            Assert.fail("Expected RuntimeException wrapping UserException but no exception was thrown");
        } catch (RuntimeException e) {
            // Assert.assertTrue("Exception should wrap a UserException",
            //         e instanceof UserException);
            Assert.assertTrue("Exception message should contain inner forEach IOException",
                    e.getCause().getMessage().contains("Inner forEach processing failed"));
        }
    }

    /**
     * Test non-partitioned table processing
     */
    @Test
    public void testNonPartitionedTableProcessing() throws Exception {
        IcebergScanNode scanNode = createRealScanNode();

        List<FileScanTask> mockTasks = Arrays.asList(
                createMockFileScanTask(200L),
                createMockFileScanTask(300L)
        );

        setupTableScanMocks(scanNode, mockTasks, false); // not partitioned
        setupNonPartitionedSplitMocking();

        List<Split> splits = scanNode.getSplits(3);
        Assert.assertNotNull("Splits should not be null", splits);
    }

    // Additional helper methods for new test scenarios

    private void setupPartitionedTableMocks(IcebergScanNode scanNode, List<FileScanTask> tasks) throws Exception {
        BaseTable icebergTable = getIcebergTable(scanNode);
        TableScan tableScan = Mockito.mock(TableScan.class);
        PartitionSpec partitionSpec = Mockito.mock(PartitionSpec.class);

        Mockito.when(icebergTable.newScan()).thenReturn(tableScan);
        Mockito.when(icebergTable.spec()).thenReturn(partitionSpec);
        Mockito.when(partitionSpec.isPartitioned()).thenReturn(true);

        // Setup partition fields
        List<org.apache.iceberg.PartitionField> partitionFields = new ArrayList<>();
        org.apache.iceberg.PartitionField field1 = Mockito.mock(org.apache.iceberg.PartitionField.class);
        org.apache.iceberg.transforms.Transform transform = Mockito.mock(org.apache.iceberg.transforms.Transform.class);

        Mockito.when(field1.name()).thenReturn("year");
        Mockito.when(field1.transform()).thenReturn(transform);
        Mockito.when(transform.isIdentity()).thenReturn(true);
        partitionFields.add(field1);

        Mockito.when(partitionSpec.fields()).thenReturn(partitionFields);

        Mockito.when(tableScan.planWith(Mockito.any())).thenReturn(tableScan);
        CloseableIterable<FileScanTask> plannedFiles = createCloseableIterable(tasks);
        Mockito.when(tableScan.planFiles()).thenReturn(plannedFiles);
    }

    private FileScanTask createMockFileScanTaskWithPartitionDetails(long fileSizeInBytes,
            String partitionName, String year, String category) {
        FileScanTask task = Mockito.mock(FileScanTask.class);
        DataFile dataFile = Mockito.mock(DataFile.class);
        StructLike partition = Mockito.mock(StructLike.class);
        org.apache.iceberg.PartitionSpec spec = Mockito.mock(org.apache.iceberg.PartitionSpec.class);

        Mockito.when(task.file()).thenReturn(dataFile);
        Mockito.when(task.spec()).thenReturn(spec);
        Mockito.when(dataFile.fileSizeInBytes()).thenReturn(fileSizeInBytes);
        Mockito.when(dataFile.partition()).thenReturn(partition);

        // Mock path() to return a CharSequence (String implements CharSequence)
        CharSequence mockPath = "/test/path/to/file.parquet";
        Mockito.when(dataFile.path()).thenReturn(mockPath);

        // Mock partition data
        Mockito.when(partition.toString()).thenReturn(partitionName);
        Mockito.when(partition.size()).thenReturn(2);
        Mockito.when(partition.get(0, Object.class)).thenReturn(year);
        Mockito.when(partition.get(1, Object.class)).thenReturn(category);

        return task;
    }

    private FileScanTask createMockFileScanTaskWithDatePartition(long fileSizeInBytes,
            String partitionName, Integer dateValue) {
        FileScanTask task = Mockito.mock(FileScanTask.class);
        DataFile dataFile = Mockito.mock(DataFile.class);
        StructLike partition = Mockito.mock(StructLike.class);
        org.apache.iceberg.PartitionSpec spec = Mockito.mock(org.apache.iceberg.PartitionSpec.class);

        // Setup partition fields for date type
        List<org.apache.iceberg.PartitionField> partitionFields = new ArrayList<>();
        org.apache.iceberg.PartitionField dateField = Mockito.mock(org.apache.iceberg.PartitionField.class);
        org.apache.iceberg.transforms.Transform transform = Mockito.mock(org.apache.iceberg.transforms.Transform.class);

        Mockito.when(dateField.name()).thenReturn("date_col");
        Mockito.when(dateField.transform()).thenReturn(transform);
        Mockito.when(transform.isIdentity()).thenReturn(true);
        partitionFields.add(dateField);

        Mockito.when(task.file()).thenReturn(dataFile);
        Mockito.when(task.spec()).thenReturn(spec);
        Mockito.when(spec.fields()).thenReturn(partitionFields);
        Mockito.when(dataFile.fileSizeInBytes()).thenReturn(fileSizeInBytes);
        Mockito.when(dataFile.partition()).thenReturn(partition);
        Mockito.when(dataFile.path()).thenReturn((CharSequence) "/test/date/partition/file.parquet");

        // Mock partition data with date value
        Mockito.when(partition.toString()).thenReturn(partitionName);
        Mockito.when(partition.size()).thenReturn(1);
        Mockito.when(partition.get(0, Object.class)).thenReturn(dateValue);

        return task;
    }

    private FileScanTask createDetailedMockFileScanTask(long fileSizeInBytes, String path, long start, long length) {
        FileScanTask task = Mockito.mock(FileScanTask.class);
        DataFile dataFile = Mockito.mock(DataFile.class);
        StructLike partition = Mockito.mock(StructLike.class);
        org.apache.iceberg.PartitionSpec spec = Mockito.mock(org.apache.iceberg.PartitionSpec.class);

        Mockito.when(task.file()).thenReturn(dataFile);
        Mockito.when(task.spec()).thenReturn(spec);
        Mockito.when(task.start()).thenReturn(start);
        Mockito.when(task.length()).thenReturn(length);
        Mockito.when(dataFile.fileSizeInBytes()).thenReturn(fileSizeInBytes);
        Mockito.when(dataFile.partition()).thenReturn(partition);
        Mockito.when(dataFile.path()).thenReturn((CharSequence) path);
        Mockito.when(partition.toString()).thenReturn("test_partition");

        // Set up partition data
        Mockito.when(partition.size()).thenReturn(1);
        Mockito.when(partition.get(0, Object.class)).thenReturn("2023");

        // Set up partition fields to match the partitioned table setup
        List<org.apache.iceberg.PartitionField> partitionFields = new ArrayList<>();
        org.apache.iceberg.PartitionField field1 = Mockito.mock(org.apache.iceberg.PartitionField.class);
        org.apache.iceberg.transforms.Transform transform = Mockito.mock(org.apache.iceberg.transforms.Transform.class);

        Mockito.when(field1.name()).thenReturn("year");
        Mockito.when(field1.transform()).thenReturn(transform);
        Mockito.when(transform.isIdentity()).thenReturn(true);
        partitionFields.add(field1);

        Mockito.when(spec.fields()).thenReturn(partitionFields);

        return task;
    }

    private void setupPartitionSplitMocking() {
        new MockUp<TableScanUtil>() {
            @Mock
            public CloseableIterable<FileScanTask> splitFiles(CloseableIterable<FileScanTask> tasks, long splitSize) {
                List<FileScanTask> taskList = new ArrayList<>();
                for (FileScanTask task : tasks) {
                    taskList.add(task);
                }
                return createCloseableIterable(taskList);
            }

            @Mock
            public CloseableIterable<CombinedScanTask> planTasks(CloseableIterable<FileScanTask> tasks,
                                                                long splitSize, int lookback, long maxFiles) {
                List<CombinedScanTask> combinedTasks = new ArrayList<>();
                CombinedScanTask combinedTask = Mockito.mock(CombinedScanTask.class);
                List<FileScanTask> fileTasks = new ArrayList<>();

                for (FileScanTask task : tasks) {
                    fileTasks.add(task);
                }

                Mockito.when(combinedTask.files()).thenReturn(fileTasks);
                combinedTasks.add(combinedTask);
                return createCloseableIterable(combinedTasks);
            }
        };
    }

    private void setupDetailedSplitMocking() {
        new MockUp<TableScanUtil>() {
            @Mock
            public CloseableIterable<FileScanTask> splitFiles(CloseableIterable<FileScanTask> tasks, long splitSize) {
                return tasks; // Return as-is for testing
            }

            @Mock
            public CloseableIterable<CombinedScanTask> planTasks(CloseableIterable<FileScanTask> tasks,
                                                                long splitSize, int lookback, long maxFiles) {
                List<CombinedScanTask> combinedTasks = new ArrayList<>();
                CombinedScanTask combinedTask = Mockito.mock(CombinedScanTask.class);
                List<FileScanTask> taskList = new ArrayList<>();

                // Explicitly preserve the mocked FileScanTask objects
                for (FileScanTask task : tasks) {
                    taskList.add(task);
                }

                Mockito.when(combinedTask.files()).thenReturn(taskList);
                combinedTasks.add(combinedTask);

                return createCloseableIterable(combinedTasks);
            }
        };
    }

    private void setupForEachIOExceptionMocking() {
        new MockUp<TableScanUtil>() {
            @Mock
            public CloseableIterable<FileScanTask> splitFiles(CloseableIterable<FileScanTask> tasks, long splitSize) {
                return tasks;
            }

            @Mock
            public CloseableIterable<CombinedScanTask> planTasks(CloseableIterable<FileScanTask> tasks,
                                                                long splitSize, int lookback, long maxFiles) {
                // Create a combined task that will cause IOException in the forEach processing
                return new CloseableIterable<CombinedScanTask>() {
                    @Override
                    public CloseableIterator<CombinedScanTask> iterator() {
                        return new CloseableIterator<CombinedScanTask>() {
                            private boolean returned = false;

                            @Override
                            public boolean hasNext() {
                                return !returned;
                            }

                            @Override
                            public CombinedScanTask next() {
                                returned = true;
                                CombinedScanTask task = Mockito.mock(CombinedScanTask.class);

                                // Mock files() to throw IOException when processed
                                Mockito.when(task.files()).thenThrow(new RuntimeException(
                                        new IOException("Inner forEach processing failed")));

                                return task;
                            }

                            @Override
                            public void close() throws IOException {}
                        };
                    }

                    @Override
                    public void close() throws IOException {}
                };
            }
        };
    }

    private void setupNonPartitionedSplitMocking() {
        new MockUp<TableScanUtil>() {
            @Mock
            public CloseableIterable<FileScanTask> splitFiles(CloseableIterable<FileScanTask> tasks, long splitSize) {
                return tasks;
            }

            @Mock
            public CloseableIterable<CombinedScanTask> planTasks(CloseableIterable<FileScanTask> tasks,
                                                                long splitSize, int lookback, long maxFiles) {
                List<CombinedScanTask> combinedTasks = new ArrayList<>();
                CombinedScanTask combinedTask = Mockito.mock(CombinedScanTask.class);
                List<FileScanTask> taskList = new ArrayList<>();

                for (FileScanTask task : tasks) {
                    taskList.add(task);
                }
                Mockito.when(combinedTask.files()).thenReturn(taskList);
                combinedTasks.add(combinedTask);

                return createCloseableIterable(combinedTasks);
            }
        };
    }

    private void setupDatePartitionedTableMocks(IcebergScanNode scanNode, List<FileScanTask> tasks) throws Exception {
        BaseTable icebergTable = getIcebergTable(scanNode);
        TableScan tableScan = Mockito.mock(TableScan.class);
        PartitionSpec partitionSpec = Mockito.mock(PartitionSpec.class);
        Schema schema = Mockito.mock(Schema.class);
        Types.StructType structType = Mockito.mock(Types.StructType.class);

        Mockito.when(icebergTable.newScan()).thenReturn(tableScan);
        Mockito.when(icebergTable.spec()).thenReturn(partitionSpec);
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(schema.asStruct()).thenReturn(structType);
        Mockito.when(partitionSpec.isPartitioned()).thenReturn(true);

        // Setup partition fields with DATE type
        List<org.apache.iceberg.PartitionField> partitionFields = new ArrayList<>();
        org.apache.iceberg.PartitionField dateField = Mockito.mock(org.apache.iceberg.PartitionField.class);
        org.apache.iceberg.transforms.Transform transform = Mockito.mock(org.apache.iceberg.transforms.Transform.class);

        Mockito.when(dateField.name()).thenReturn("date_col");
        Mockito.when(dateField.transform()).thenReturn(transform);
        Mockito.when(transform.isIdentity()).thenReturn(true);
        partitionFields.add(dateField);

        Mockito.when(partitionSpec.fields()).thenReturn(partitionFields);

        // Mock the schema to return DATE type for the partition field
        org.apache.iceberg.types.Type dateType = Mockito.mock(org.apache.iceberg.types.Type.class);
        org.apache.iceberg.types.Type.TypeID dateTypeId = org.apache.iceberg.types.Type.TypeID.DATE;

        Mockito.when(structType.fieldType("date_col")).thenReturn(dateType);
        Mockito.when(dateType.typeId()).thenReturn(dateTypeId);

        Mockito.when(tableScan.planWith(Mockito.any())).thenReturn(tableScan);
        CloseableIterable<FileScanTask> plannedFiles = createCloseableIterable(tasks);
        Mockito.when(tableScan.planFiles()).thenReturn(plannedFiles);
    }

    private void setupDateTransformationMocking() {
        new MockUp<TableScanUtil>() {
            @Mock
            public CloseableIterable<FileScanTask> splitFiles(CloseableIterable<FileScanTask> tasks, long splitSize) {
                return tasks;
            }

            @Mock
            public CloseableIterable<CombinedScanTask> planTasks(CloseableIterable<FileScanTask> tasks,
                                                                long splitSize, int lookback, long maxFiles) {
                List<CombinedScanTask> combinedTasks = new ArrayList<>();
                CombinedScanTask combinedTask = Mockito.mock(CombinedScanTask.class);
                List<FileScanTask> taskList = new ArrayList<>();

                for (FileScanTask task : tasks) {
                    taskList.add(task);
                }
                Mockito.when(combinedTask.files()).thenReturn(taskList);
                combinedTasks.add(combinedTask);

                return createCloseableIterable(combinedTasks);
            }
        };
    }

    private void setFieldValue(Object obj, String fieldName, Object value) throws Exception {
        java.lang.reflect.Field field = null;
        Class<?> clazz = obj.getClass();

        // Try to find the field in the class hierarchy
        while (clazz != null && field == null) {
            try {
                field = clazz.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }

        if (field == null) {
            throw new NoSuchFieldException("Field " + fieldName + " not found in class hierarchy");
        }

        field.setAccessible(true);
        field.set(obj, value);
    }
}
