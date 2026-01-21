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

package org.apache.doris.datasource.hive.source;


import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.FileScanNode;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.FileCacheValue;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.BDPAuthContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class HiveScanNodeTest {

    @BeforeClass
    public static void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    @Test
    public void testDefaultMaxSelectedTotalFileSize(
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog) {
        SessionVariable sessionVariable = new SessionVariable();
        // Default is 8796093022208L now
        Assertions.assertEquals(8796093022208L, sessionVariable.maxSelectedTotalFileSizeForHiveTable);

        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;
                tupleDesc.getId();
                result = new TupleId(1);
                table.getCatalog();
                result = catalog;
                catalog.bindBrokerName();
                result = "test";
                table.isOrcOrParquetFileFormat();
                result = true;
                table.getDbName();
                result = "test";
                table.getName();
                result = "test";
            }
        };

        HiveScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);

        List<FileCacheValue> fileCaches = new ArrayList<>();
        FileCacheValue fileCache1 = new FileCacheValue();
        // 8TB + 1 byte
        RemoteFile file1 = new RemoteFile("file1", true, 8796093022208L + 1, 1024);
        file1.setPath(new Path("file1.text"));
        fileCache1.addFile(file1, new LocationPath("file1.text"));
        fileCaches.add(fileCache1);

        // Should fail because default logic uses 8TB limit when variable is -1
        try {
            scanNode.getSelectedFileSize(fileCaches);
            Assertions.fail("Should throw exception when session variable limit is exceeded");
        } catch (AnalysisException e) {
            Assertions.assertTrue(e.getMessage().contains("exceed max bytes for single hive table"));
        } catch (Exception e) {
            Assertions.fail(e);
        }

        // Set to larger value, should pass
        sessionVariable.maxSelectedTotalFileSizeForHiveTable = 8796093022208L + 100;
        try {
            scanNode.getSelectedFileSize(fileCaches);
        } catch (Exception e) {
            Assertions.fail(e);
        }

        // Reset and test unrecommended
        sessionVariable.maxSelectedTotalFileSizeForHiveTable = -1;
        new Expectations() {
            {
                table.isOrcOrParquetFileFormat();
                result = false;
            }
        };
        try {
            scanNode.getSelectedFileSize(fileCaches);
            Assertions.fail("Should throw exception for unrecommended file size");
        } catch (AnalysisException e) {
            Assertions.assertTrue(e.getMessage().contains("exceed max bytes for single hive table with unrecommended"));
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testGetFileSplitSize(
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog) {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.maxSelectedFileSizeForUnrecommendedHiveTable = 5000000000000L;
        sessionVariable.maxSelectedTotalFileSizeForHiveTable = 60000000000000L;
        List<FileCacheValue> fileCaches = new ArrayList<>();
        FileCacheValue fileCache1 = new FileCacheValue();
        fileCache1.setSplittable(true);
        RemoteFile file1 = new RemoteFile("file1", true, 1024, 1024);
        file1.setPath(new Path("file1.text"));
        fileCache1.addFile(file1, new LocationPath("file1.text"));
        fileCaches.add(fileCache1);
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                catalog.bindBrokerName();
                result = "test";
            }
        };
        Config.file_size_range_to_decide_split_size = new long[] {20 * 1024 * 1024 * 1024L, 40 * 1024 * 1024 * 1024L,
                80 * 1024 * 1024 * 1024L, 160 * 1024 * 1024 * 1024L, 320 * 1024 * 1024 * 1024L};
        HiveScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            context.resetTotalScanBytes();
            MetricRepo.COUNTER_HMS_SCAN_SIZE_BYTES.reset();
            long fileSplitSize = scanNode.getFileSplitSize(fileCaches, false);
            Assertions.assertEquals(FileScanNode.DEFAULT_SPLIT_SIZE, fileSplitSize);
            Assertions.assertEquals(1024, context.getTotalScanBytes());
            Assertions.assertEquals(1024, MetricRepo.COUNTER_HMS_SCAN_SIZE_BYTES.getValue().longValue());
        } catch (Exception e) {
            Assertions.fail(e);
        }

        try {
            context.resetTotalScanBytes();
            MetricRepo.COUNTER_HMS_SCAN_SIZE_BYTES.reset();
            long fileSplitSize = scanNode.getFileSplitSize(fileCaches, true);
            Assertions.assertEquals(FileScanNode.TINY_SPLIT_FILE_SIZE, fileSplitSize);
            Assertions.assertEquals(1024, context.getTotalScanBytes());
            Assertions.assertEquals(1024, MetricRepo.COUNTER_HMS_SCAN_SIZE_BYTES.getValue().longValue());
        } catch (Exception e) {
            Assertions.fail(e);
        }

        try {
            context.resetTotalScanBytes();
            MetricRepo.COUNTER_HMS_SCAN_SIZE_BYTES.reset();
            fileCaches.get(0).getFiles().get(0).setLength(Config.file_size_range_to_decide_split_size[0] + 1);
            long fileSplitSize = scanNode.getFileSplitSize(fileCaches, true);
            Assertions.assertEquals(FileScanNode.SMALL_SPLIT_FILE_SIZE, fileSplitSize);
            Assertions.assertEquals(Config.file_size_range_to_decide_split_size[0] + 1, context.getTotalScanBytes());
            Assertions.assertEquals(Config.file_size_range_to_decide_split_size[0] + 1,
                    MetricRepo.COUNTER_HMS_SCAN_SIZE_BYTES.getValue().longValue());
        } catch (Exception e) {
            Assertions.fail(e);
        }

        try {
            context.resetTotalScanBytes();
            MetricRepo.COUNTER_HMS_SCAN_SIZE_BYTES.reset();
            fileCaches.get(0).getFiles().get(0).setLength(Config.file_size_range_to_decide_split_size[1] + 1);
            long fileSplitSize = scanNode.getFileSplitSize(fileCaches, true);
            Assertions.assertEquals(FileScanNode.MEDIUM_SPLIT_FILE_SIZE, fileSplitSize);
            Assertions.assertEquals(Config.file_size_range_to_decide_split_size[1] + 1, context.getTotalScanBytes());
            Assertions.assertEquals(Config.file_size_range_to_decide_split_size[1] + 1,
                    MetricRepo.COUNTER_HMS_SCAN_SIZE_BYTES.getValue().longValue());
        } catch (Exception e) {
            Assertions.fail(e);
        }

        try {
            context.resetTotalScanBytes();
            MetricRepo.COUNTER_HMS_SCAN_SIZE_BYTES.reset();
            fileCaches.get(0).getFiles().get(0).setLength(Config.file_size_range_to_decide_split_size[2] + 1);
            long fileSplitSize = scanNode.getFileSplitSize(fileCaches, true);
            Assertions.assertEquals(FileScanNode.LARGE_SPLIT_FILE_SIZE, fileSplitSize);
            Assertions.assertEquals(Config.file_size_range_to_decide_split_size[2] + 1, context.getTotalScanBytes());
            Assertions.assertEquals(Config.file_size_range_to_decide_split_size[2] + 1,
                    MetricRepo.COUNTER_HMS_SCAN_SIZE_BYTES.getValue().longValue());
        } catch (Exception e) {
            Assertions.fail(e);
        }

        try {
            context.resetTotalScanBytes();
            MetricRepo.COUNTER_HMS_SCAN_SIZE_BYTES.reset();
            fileCaches.get(0).getFiles().get(0).setLength(Config.file_size_range_to_decide_split_size[3] + 1);
            long fileSplitSize = scanNode.getFileSplitSize(fileCaches, true);
            Assertions.assertEquals(FileScanNode.HUGE_SPLIT_FILE_SIZE, fileSplitSize);
            Assertions.assertEquals(Config.file_size_range_to_decide_split_size[3] + 1, context.getTotalScanBytes());
            Assertions.assertEquals(Config.file_size_range_to_decide_split_size[3] + 1,
                    MetricRepo.COUNTER_HMS_SCAN_SIZE_BYTES.getValue().longValue());
        } catch (Exception e) {
            Assertions.fail(e);
        }

        try {
            context.resetTotalScanBytes();
            MetricRepo.COUNTER_HMS_SCAN_SIZE_BYTES.reset();
            fileCaches.get(0).getFiles().get(0).setLength(Config.file_size_range_to_decide_split_size[4] + 1);
            long fileSplitSize = scanNode.getFileSplitSize(fileCaches, true);
            Assertions.assertEquals(FileScanNode.DEFAULT_SPLIT_SIZE, fileSplitSize);
            Assertions.assertEquals(Config.file_size_range_to_decide_split_size[4] + 1, context.getTotalScanBytes());
            Assertions.assertEquals(Config.file_size_range_to_decide_split_size[4] + 1,
                    MetricRepo.COUNTER_HMS_SCAN_SIZE_BYTES.getValue().longValue());
        } catch (Exception e) {
            Assertions.fail(e);
        }

        try {
            // Test session variable for file size limit
            sessionVariable.maxSelectedTotalFileSizeForHiveTable = 500;
            try {
                scanNode.getSelectedFileSize(fileCaches);
                Assertions.fail("Should throw exception when session variable limit is exceeded");
            } catch (AnalysisException e) {
                Assertions.assertTrue(e.getMessage().contains("exceed max bytes for single hive table"));
            }
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testNormalGenerateFileSplits(
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog) throws IOException, AnalysisException {
        SessionVariable sessionVariable = new SessionVariable();
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                catalog.bindBrokerName();
                result = "test";

                table.getPartitionUpdateTime();
                result = 0;

                table.setPartitionUpdateTime(10);
                minTimes = 0;
            }
        };
        HiveScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);
        List<Split> allFiles = new ArrayList<>();
        List<FileCacheValue> fileCaches = new ArrayList<>();
        FileCacheValue fileCache1 = new FileCacheValue();
        fileCache1.setSplittable(true);
        RemoteFile file1 = new RemoteFile("file1", true, 3072, 1024);
        file1.setPath(new Path("file1.text"));
        fileCache1.addFile(file1, new LocationPath("file1.text"));
        FileCacheValue fileCache2 = new FileCacheValue();
        fileCache2.setSplittable(true);
        RemoteFile file2 = new RemoteFile("file2", true, 2048, 1024);
        file2.setPath(new Path("file2"));
        fileCache2.addFile(file2, new LocationPath("file2.text"));
        FileCacheValue fileCache3 = new FileCacheValue();
        fileCache3.setSplittable(true);
        RemoteFile file3 = new RemoteFile("file3", true, 1024, 1024);
        file3.setPath(new Path("file3"));
        fileCache3.addFile(file3, new LocationPath("file3.text"));
        fileCaches.add(fileCache1);
        fileCaches.add(fileCache2);
        fileCaches.add(fileCache3);
        scanNode.generateFileSplits(allFiles, fileCaches, false, 1024);
        Assert.assertEquals(3, allFiles.size());
        Assert.assertEquals("file1.text", allFiles.get(0).getPathString());
        Assert.assertEquals(3072, allFiles.get(0).getLength());
        Assert.assertEquals("file2.text", allFiles.get(1).getPathString());
        Assert.assertEquals(2048, allFiles.get(1).getLength());
        Assert.assertEquals("file3.text", allFiles.get(2).getPathString());
        Assert.assertEquals(1024, allFiles.get(2).getLength());
    }

    @Test
    public void testGetSelectedFileSize(
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog) {
        SessionVariable sessionVariable = new SessionVariable();
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                catalog.bindBrokerName();
                result = "test";

                table.isOrcOrParquetFileFormat();
                result = true;

                table.getDbName();
                result = "test";

                table.getName();
                result = "test";
            }
        };
        List<FileCacheValue> fileCaches = new ArrayList<>();
        FileCacheValue fileCache1 = new FileCacheValue();
        fileCache1.setSplittable(true);
        RemoteFile file1 = new RemoteFile("file1", true, 3072, 1024);
        file1.setPath(new Path("file1"));
        fileCache1.addFile(file1, new LocationPath("file1.text"));
        FileCacheValue fileCache2 = new FileCacheValue();
        fileCache2.setSplittable(true);
        RemoteFile file2 = new RemoteFile("file2", true, 2048, 1024);
        file2.setPath(new Path("file2"));
        fileCache2.addFile(file2, new LocationPath("file2.text"));
        FileCacheValue fileCache3 = new FileCacheValue();
        fileCache3.setSplittable(true);
        RemoteFile file3 = new RemoteFile("file3", true, 1024, 1024);
        file3.setPath(new Path("file3"));
        fileCache3.addFile(file3, new LocationPath("file3.text"));
        fileCaches.add(fileCache1);
        fileCaches.add(fileCache2);
        fileCaches.add(fileCache3);
        HiveScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);
        try {
            long selectedFileSize = scanNode.getSelectedFileSize(fileCaches);
            Assertions.assertEquals(6144, selectedFileSize);
        } catch (Exception e) {
            Assertions.fail(e);
        }
        sessionVariable.maxSelectedFileSizeForUnrecommendedHiveTable = 5000;
        sessionVariable.maxSelectedTotalFileSizeForHiveTable = 6000;
        try {
            scanNode.getSelectedFileSize(fileCaches);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertEquals("errCode = 2, detailMessage = the total scan bytes: 6144 for test.test"
                    + " has exceed max bytes for single hive table: 6000", e.getMessage());
        }
        new Expectations() {
            {
                table.isOrcOrParquetFileFormat();
                result = false;
            }
        };
        try {
            scanNode.getSelectedFileSize(fileCaches);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertEquals("errCode = 2, detailMessage = the total scan bytes: 6144 for test.test"
                    + " has exceed max bytes for single hive table with unrecommended file format: 5000",
                    e.getMessage());
        }

    }



    @Test
    public void testGenerateFileSplitsWithSortByFileSize(
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog) throws IOException, AnalysisException {
        SessionVariable sessionVariable = new SessionVariable();
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                catalog.bindBrokerName();
                result = "test";

                table.getPartitionUpdateTime();
                result = 0;

                table.setPartitionUpdateTime(10);
                minTimes = 0;
            }
        };
        HiveScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);
        List<Split> allFiles = new ArrayList<>();
        List<FileCacheValue> fileCaches = new ArrayList<>();
        FileCacheValue fileCache1 = new FileCacheValue();
        fileCache1.setSplittable(true);
        RemoteFile file1 = new RemoteFile("file1", true, 3072, 1024);
        file1.setPath(new Path("file1"));
        fileCache1.addFile(file1, new LocationPath("file1.text"));
        FileCacheValue fileCache2 = new FileCacheValue();
        fileCache2.setSplittable(true);
        RemoteFile file2 = new RemoteFile("file2", true, 2048, 1024);
        file2.setPath(new Path("file2"));
        fileCache2.addFile(file2, new LocationPath("file2.text"));
        FileCacheValue fileCache3 = new FileCacheValue();
        fileCache3.setSplittable(true);
        RemoteFile file3 = new RemoteFile("file3", true, 1024, 1024);
        file3.setPath(new Path("file3"));
        fileCache3.addFile(file3, new LocationPath("file3.text"));
        fileCaches.add(fileCache1);
        fileCaches.add(fileCache2);
        fileCaches.add(fileCache3);
        scanNode.generateFileSplits(allFiles, fileCaches, true, 1024);
        Assert.assertEquals(6, allFiles.size());
        Assert.assertEquals("file3.text", allFiles.get(0).getPathString());
        Assert.assertEquals(1024, allFiles.get(0).getLength());
        Assert.assertEquals("file2.text", allFiles.get(1).getPathString());
        Assert.assertEquals(1024, allFiles.get(1).getLength());
        Assert.assertEquals("file2.text", allFiles.get(2).getPathString());
        Assert.assertEquals(1024, allFiles.get(2).getLength());
        Assert.assertEquals("file1.text", allFiles.get(3).getPathString());
        Assert.assertEquals(1024, allFiles.get(3).getLength());
        Assert.assertEquals("file1.text", allFiles.get(4).getPathString());
        Assert.assertEquals(1024, allFiles.get(4).getLength());
        Assert.assertEquals("file1.text", allFiles.get(5).getPathString());
        Assert.assertEquals(1024, allFiles.get(5).getLength());
    }

    @Test
    public void testCheckSelectedPartitionNumLimit(
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog) {
        SessionVariable sessionVariable = new SessionVariable();
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                catalog.bindBrokerName();
                result = "test";

                table.getDlaType();
                result = HMSExternalTable.DLAType.HUDI;

                table.getDbName();
                result = "test";

                table.getName();
                result = "test";
            }
        };
        HiveScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);
        scanNode.setSelectedPartitionNum(100);
        int oldMax = Config.max_selected_partition_num_for_lakehouse_table;
        Config.max_selected_partition_num_for_lakehouse_table = 10;
        try {
            scanNode.checkSelectedPartitionNumLimit();
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertEquals("errCode = 2, detailMessage = the selected partition num:"
                    + " 100 for test.test has exceed max selected partition num for single Hudi table: 10",
                    e.getMessage());
        }
        Config.max_selected_partition_num_for_lakehouse_table = oldMax;

        new Expectations() {
            {
                table.getDlaType();
                result = HMSExternalTable.DLAType.HIVE;
            }
        };

        oldMax = Config.max_selected_partition_num_for_hive_table;
        Config.max_selected_partition_num_for_hive_table = 50;
        try {
            scanNode.checkSelectedPartitionNumLimit();
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertEquals("errCode = 2, detailMessage = the selected partition num:"
                            + " 100 for test.test has exceed max selected partition num for single Hive table: 50",
                    e.getMessage());
        }
        Config.max_selected_partition_num_for_hive_table = oldMax;
    }

    // ========== Tests for logIfGetNoFileFromEmptyPartitions ==========

    /**
     * Thread-safe test appender for capturing log events
     */
    private static class TestAppender extends AbstractAppender {
        private final List<LogEvent> events = new CopyOnWriteArrayList<>();

        public TestAppender(String name) {
            super(name, null, PatternLayout.createDefaultLayout(), true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable()); // Store immutable copy for thread safety
        }

        public List<LogEvent> getEvents() {
            return new ArrayList<>(events);
        }

        public void clearEvents() {
            events.clear();
        }

        public List<String> getFormattedMessages() {
            return events.stream()
                    .map(event -> event.getMessage().getFormattedMessage())
                    .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        }

        public boolean hasLoggedMessage(String expectedMessage) {
            return events.stream()
                    .anyMatch(event -> event.getMessage().getFormattedMessage().contains(expectedMessage));
        }

        public long getEventCount(Level level) {
            return events.stream()
                    .filter(event -> event.getLevel().equals(level))
                    .count();
        }
    }

    private boolean originalConfigValue;
    private TestAppender testAppender;
    private Logger logger;

    @Before
    public void setUpLogTest() {
        // Save original config value
        originalConfigValue = Config.enable_log_empty_partition_when_list_file;

        // Get the specific logger for HiveScanNode
        logger = (Logger) LogManager.getLogger(HiveScanNode.class);

        // Create and configure test appender
        testAppender = new TestAppender("TestAppender");
        testAppender.start();

        // Add appender to logger
        logger.addAppender(testAppender);
        logger.setLevel(Level.INFO); // Ensure INFO level is captured
    }

    @After
    public void tearDownLogTest() {
        // Restore original config value
        Config.enable_log_empty_partition_when_list_file = originalConfigValue;

        // Clear thread local BDPAuthContext
        BDPAuthContext.clear();

        // Clean up: remove test appender
        if (logger != null && testAppender != null) {
            logger.removeAppender(testAppender);
            testAppender.stop();
        }
    }

    @Test
    public void testLogEmptyPartitionWithEmptyFiles(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog,
            @Mocked HivePartition partition) {
        // Setup: Enable logging
        Config.enable_log_empty_partition_when_list_file = true;

        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                catalog.bindBrokerName();
                result = "test";

                table.getDbName();
                result = "test_db";

                table.getName();
                result = "test_table";

                partition.getPath();
                result = "/user/hive/warehouse/test_db.db/test_table/partition1";

                partition.getLastModifiedTime();
                result = 1609459200000L; // 2021-01-01 00:00:00
            }
        };

        HiveScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);

        // Create empty file cache
        List<FileCacheValue> fileCaches = new ArrayList<>();
        FileCacheValue emptyFileCache = new FileCacheValue();
        fileCaches.add(emptyFileCache);

        List<HivePartition> partitions = new ArrayList<>();
        partitions.add(partition);

        // Test: Call method with empty partition
        scanNode.logIfGetNoFileFromEmptyPartitions(fileCaches, partitions);

        // Verify: Should log for empty partition
        Assertions.assertEquals(1, testAppender.getEventCount(Level.INFO),
                "Should generate one INFO log event for empty partition");

        String logMessage = testAppender.getFormattedMessages().get(0);
        Assertions.assertTrue(logMessage.contains("/user/hive/warehouse/test_db.db/test_table/partition1"),
                "Log should contain partition path");
        Assertions.assertTrue(logMessage.contains("test_db"),
                "Log should contain database name");
        Assertions.assertTrue(logMessage.contains("test_table"),
                "Log should contain table name");
    }

    @Test
    public void testLogEmptyPartitionWithNonEmptyFiles(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog,
            @Mocked HivePartition partition) {
        // Setup: Enable logging
        Config.enable_log_empty_partition_when_list_file = true;

        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                catalog.bindBrokerName();
                result = "test";
            }
        };

        HiveScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);

        // Create non-empty file cache
        List<FileCacheValue> fileCaches = new ArrayList<>();
        FileCacheValue fileCache = new FileCacheValue();
        RemoteFile file = new RemoteFile("file1", true, 1024, 1024);
        file.setPath(new Path("file1.parquet"));
        fileCache.addFile(file, new LocationPath("file1.parquet"));
        fileCaches.add(fileCache);

        List<HivePartition> partitions = new ArrayList<>();
        partitions.add(partition);

        // Test: Call method with non-empty partition
        scanNode.logIfGetNoFileFromEmptyPartitions(fileCaches, partitions);

        // Verify: Should NOT log for non-empty partition
        Assertions.assertEquals(0, testAppender.getEvents().size(),
                "Should not generate log events for non-empty partition");
    }

    @Test
    public void testLogEmptyPartitionWithMultipleEmptyPartitions(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog,
            @Mocked HivePartition partition1,
            @Mocked HivePartition partition2) {
        // Setup: Enable logging
        Config.enable_log_empty_partition_when_list_file = true;

        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                catalog.bindBrokerName();
                result = "test";

                table.getDbName();
                result = "test_db";

                table.getName();
                result = "test_table";

                partition1.getPath();
                result = "/path/partition1";

                partition1.getLastModifiedTime();
                result = 1609459200000L;

                partition2.getPath();
                result = "/path/partition2";

                partition2.getLastModifiedTime();
                result = 1609545600000L;
            }
        };

        HiveScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);

        // Create multiple empty file caches
        List<FileCacheValue> fileCaches = new ArrayList<>();
        FileCacheValue emptyCache1 = new FileCacheValue();
        FileCacheValue emptyCache2 = new FileCacheValue();
        fileCaches.add(emptyCache1);
        fileCaches.add(emptyCache2);

        List<HivePartition> partitions = new ArrayList<>();
        partitions.add(partition1);
        partitions.add(partition2);

        // Test: Call method with multiple empty partitions
        scanNode.logIfGetNoFileFromEmptyPartitions(fileCaches, partitions);

        // Verify: Should log for all empty partitions
        Assertions.assertEquals(2, testAppender.getEventCount(Level.INFO),
                "Should generate 2 INFO log events for 2 empty partitions");

        List<String> messages = testAppender.getFormattedMessages();
        Assertions.assertTrue(messages.get(0).contains("/path/partition1"),
                "First log should be for partition1");
        Assertions.assertTrue(messages.get(1).contains("/path/partition2"),
                "Second log should be for partition2");
    }

    @Test
    public void testLogEmptyPartitionWithMixedPartitions(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog,
            @Mocked HivePartition partition1,
            @Mocked HivePartition partition2,
            @Mocked HivePartition partition3) {
        // Setup: Enable logging
        Config.enable_log_empty_partition_when_list_file = true;

        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                catalog.bindBrokerName();
                result = "test";

                table.getDbName();
                result = "test_db";

                table.getName();
                result = "test_table";

                partition1.getPath();
                result = "/path/partition1";

                partition1.getLastModifiedTime();
                result = 1609459200000L;

                partition3.getPath();
                result = "/path/partition3";

                partition3.getLastModifiedTime();
                result = 1609459200000L;
            }
        };

        HiveScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);

        // Create mixed file caches: empty, non-empty, empty
        List<FileCacheValue> fileCaches = new ArrayList<>();

        // Partition 1: Empty
        FileCacheValue emptyCache1 = new FileCacheValue();
        fileCaches.add(emptyCache1);

        // Partition 2: Non-empty
        FileCacheValue nonEmptyCache = new FileCacheValue();
        RemoteFile file = new RemoteFile("file2", true, 1024, 1024);
        file.setPath(new Path("file2.parquet"));
        nonEmptyCache.addFile(file, new LocationPath("file2.parquet"));
        fileCaches.add(nonEmptyCache);

        // Partition 3: Empty
        FileCacheValue emptyCache2 = new FileCacheValue();
        fileCaches.add(emptyCache2);

        List<HivePartition> partitions = new ArrayList<>();
        partitions.add(partition1);
        partitions.add(partition2);
        partitions.add(partition3);

        // Test: Call method with mixed partitions
        scanNode.logIfGetNoFileFromEmptyPartitions(fileCaches, partitions);

        // Verify: Should log for 2 empty partitions only
        Assertions.assertEquals(2, testAppender.getEventCount(Level.INFO),
                "Should generate 2 INFO log events for 2 empty partitions");

        List<String> messages = testAppender.getFormattedMessages();
        Assertions.assertTrue(messages.get(0).contains("/path/partition1"),
                "First log should be for partition1");
        Assertions.assertTrue(messages.get(1).contains("/path/partition3"),
                "Second log should be for partition3");
    }

    @Test
    public void testLogEmptyPartitionWithBDPAuthContext(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog,
            @Mocked HivePartition partition) {
        // Setup: Enable logging
        Config.enable_log_empty_partition_when_list_file = true;

        // Set up BDPAuthContext
        BDPAuthContext authContext = new BDPAuthContext("test_user", "test_cluster", "test_tenant", "token123");
        authContext.setThreadLocalInfo();

        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                catalog.bindBrokerName();
                result = "test";

                table.getDbName();
                result = "test_db";

                table.getName();
                result = "test_table";

                partition.getPath();
                result = "/path/partition1";

                partition.getLastModifiedTime();
                result = 1609459200000L;
            }
        };

        HiveScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);

        // Create empty file cache
        List<FileCacheValue> fileCaches = new ArrayList<>();
        FileCacheValue emptyCache = new FileCacheValue();
        fileCaches.add(emptyCache);

        List<HivePartition> partitions = new ArrayList<>();
        partitions.add(partition);

        // Test: Call method with BDPAuthContext present
        scanNode.logIfGetNoFileFromEmptyPartitions(fileCaches, partitions);

        // Verify: Should log with context information (not N/A)
        Assertions.assertEquals(1, testAppender.getEventCount(Level.INFO),
                "Should generate one INFO log event");

        String logMessage = testAppender.getFormattedMessages().get(0);
        // When BDPAuthContext is present, should not contain N/A
        Assertions.assertFalse(logMessage.contains("HMS client information: N/A"),
                "Context should not be N/A when BDPAuthContext is present");
    }

    @Test
    public void testLogEmptyPartitionWithoutBDPAuthContext(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog,
            @Mocked HivePartition partition) {
        // Setup: Enable logging
        Config.enable_log_empty_partition_when_list_file = true;

        // Ensure no BDPAuthContext
        BDPAuthContext.clear();

        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                catalog.bindBrokerName();
                result = "test";

                table.getDbName();
                result = "test_db";

                table.getName();
                result = "test_table";

                partition.getPath();
                result = "/path/partition1";

                partition.getLastModifiedTime();
                result = 1609459200000L;
            }
        };

        HiveScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);

        // Create empty file cache
        List<FileCacheValue> fileCaches = new ArrayList<>();
        FileCacheValue emptyCache = new FileCacheValue();
        fileCaches.add(emptyCache);

        List<HivePartition> partitions = new ArrayList<>();
        partitions.add(partition);

        // Test: Call method without BDPAuthContext
        scanNode.logIfGetNoFileFromEmptyPartitions(fileCaches, partitions);

        // Verify: Should log with N/A for context
        Assertions.assertEquals(1, testAppender.getEventCount(Level.INFO),
                "Should generate one INFO log event");

        String logMessage = testAppender.getFormattedMessages().get(0);
        Assertions.assertTrue(logMessage.contains("HMS client information: N/A"),
                "Context should be N/A when BDPAuthContext is null");
    }

    @Test
    public void testLogEmptyPartitionCompleteMessageFormat(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog,
            @Mocked HivePartition partition) {
        // Setup: Enable logging
        Config.enable_log_empty_partition_when_list_file = true;
        BDPAuthContext.clear(); // Ensure predictable N/A values

        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                catalog.bindBrokerName();
                result = "test";

                table.getDbName();
                result = "analytics_db";

                table.getName();
                result = "events_table";

                partition.getPath();
                result = "/warehouse/analytics_db/events_table/dt=2025-01-01";

                partition.getLastModifiedTime();
                result = 1735689600000L; // 2025-01-01
            }
        };

        HiveScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);

        // Create empty file cache
        List<FileCacheValue> fileCaches = new ArrayList<>();
        FileCacheValue emptyCache = new FileCacheValue();
        fileCaches.add(emptyCache);

        List<HivePartition> partitions = new ArrayList<>();
        partitions.add(partition);

        // Test: Call method
        scanNode.logIfGetNoFileFromEmptyPartitions(fileCaches, partitions);

        // Verify complete log structure
        String logMessage = testAppender.getFormattedMessages().get(0);

        // Check all expected components are present
        Assertions.assertTrue(logMessage.contains("/warehouse/analytics_db/events_table/dt=2025-01-01"),
                "Should contain partition path");
        Assertions.assertTrue(logMessage.contains("partition last modified time 1735689600000"),
                "Should contain last modified time");
        Assertions.assertTrue(logMessage.contains("dbName analytics_db"),
                "Should contain database name");
        Assertions.assertTrue(logMessage.contains("tableName events_table"),
                "Should contain table name");
        Assertions.assertTrue(logMessage.contains("HMS client information: N/A"),
                "Should contain HMS client info (N/A in test)");
    }

    // ========== Tests for getFileSplitByPartitions (lines 288, 329, 437) ==========

    @Test
    public void testGetFileSplitByPartitionsWithCache(@Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog,
            @Injectable HiveMetaStoreCache cache,
            @Injectable org.apache.hadoop.hive.metastore.api.Table remoteTable,
            @Injectable StorageDescriptor sd,
            @Injectable SerDeInfo serDeInfo) throws Exception {
        Map<String, String> tableParams = new java.util.HashMap<>();
        tableParams.put("doris_x.enable_external_file_cache", "true");

        Map<String, String> serdeParams = new java.util.HashMap<>();
        SessionVariable sessionVariable = new SessionVariable();
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                catalog.bindBrokerName();
                result = "test";

                table.getRemoteTable();
                result = remoteTable;

                remoteTable.getParameters();
                result = tableParams;

                // These are only called when key is NOT in table params
                remoteTable.getSd();
                result = sd;
                minTimes = 0;

                sd.getSerdeInfo();
                result = serDeInfo;
                minTimes = 0;

                sd.getInputFormat();
                result = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
                minTimes = 0;

                serDeInfo.getParameters();
                result = serdeParams;
                minTimes = 0;
            }
        };

        HiveScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);

        // Create file cache values
        List<FileCacheValue> fileCaches = new ArrayList<>();
        FileCacheValue fileCache = new FileCacheValue();
        fileCache.setSplittable(true);
        RemoteFile file = new RemoteFile("file1", true, 1024, 1024);
        file.setPath(new Path("file1.parquet"));
        fileCache.addFile(file, new LocationPath("file1.parquet"));
        fileCaches.add(fileCache);

        // Create partition
        HivePartition partition = new HivePartition("testDb", "testTable", false,
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "hdfs://ns6666/user/hive/warehouse/test.db/test_table/a=dd",
                java.util.Arrays.asList("dd"), null);

        new Expectations() {
            {
                cache.getFilesByPartitions((List<HivePartition>) any, anyBoolean, anyString);
                result = fileCaches;
            }
        };

        List<Split> allFiles = new ArrayList<>();

        // Call the method that triggers line 437
        scanNode.getFileSplitByPartitions(cache, java.util.Arrays.asList(partition), allFiles, "test", true);

        // Verify files were processed
        Assertions.assertFalse(allFiles.isEmpty(), "Should have generated file splits");
    }

    @Test
    public void testGetFileSplitByPartitionsWithoutCache(@Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog,
            @Injectable HiveMetaStoreCache cache,
            @Injectable org.apache.hadoop.hive.metastore.api.Table remoteTable,
            @Injectable StorageDescriptor sd,
            @Injectable SerDeInfo serDeInfo) throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        Map<String, String> tableParams = new java.util.HashMap<>();
        tableParams.put("doris_x.enable_external_file_cache", "false");

        Map<String, String> serdeParams = new java.util.HashMap<>();

        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                catalog.bindBrokerName();
                result = "test";

                table.getRemoteTable();
                result = remoteTable;

                remoteTable.getParameters();
                result = tableParams;

                // These are only called when key is NOT in table params
                remoteTable.getSd();
                result = sd;
                minTimes = 0;

                sd.getSerdeInfo();
                result = serDeInfo;
                minTimes = 0;

                sd.getInputFormat();
                result = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
                minTimes = 0;

                serDeInfo.getParameters();
                result = serdeParams;
                minTimes = 0;
            }
        };

        HiveScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);

        // Create file cache values
        List<FileCacheValue> fileCaches = new ArrayList<>();
        FileCacheValue fileCache = new FileCacheValue();
        fileCache.setSplittable(true);
        RemoteFile file = new RemoteFile("file1", true, 1024, 1024);
        file.setPath(new Path("file1.parquet"));
        fileCache.addFile(file, new LocationPath("file1.parquet"));
        fileCaches.add(fileCache);

        // Create partition
        HivePartition partition = new HivePartition("testDb", "testTable", false,
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "hdfs://ns6666/user/hive/warehouse/test.db/test_table/a=dd",
                java.util.Arrays.asList("dd"), null);

        new Expectations() {
            {
                cache.getFilesByPartitions((List<HivePartition>) any, anyBoolean, anyString);
                result = fileCaches;
            }
        };

        List<Split> allFiles = new ArrayList<>();

        // Call the method that triggers line 437 with cache disabled
        scanNode.getFileSplitByPartitions(cache, java.util.Arrays.asList(partition), allFiles, "test", false);

        // Verify files were processed
        Assertions.assertFalse(allFiles.isEmpty(), "Should have generated file splits");
    }

    @Test
    public void testGetFileSplitByPartitionsWithSerdeParams(@Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog,
            @Injectable HiveMetaStoreCache cache,
            @Injectable org.apache.hadoop.hive.metastore.api.Table remoteTable,
            @Injectable StorageDescriptor sd,
            @Injectable SerDeInfo serDeInfo) throws Exception {
        // Test line 437: cache.getFilesByPartitions when cache setting is in serde params

        Map<String, String> tableParams = new java.util.HashMap<>();
        // No doris_x.enable_external_file_cache in table params

        Map<String, String> serdeParams = new java.util.HashMap<>();
        serdeParams.put("doris_x.enable_external_file_cache", "true");

        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                catalog.bindBrokerName();
                result = "test";

                table.getRemoteTable();
                result = remoteTable;

                remoteTable.getParameters();
                result = tableParams;

                remoteTable.getSd();
                result = sd;

                sd.getSerdeInfo();
                result = serDeInfo;

                sd.getInputFormat();
                result = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
                minTimes = 0;

                serDeInfo.getParameters();
                result = serdeParams;
            }
        };
        SessionVariable sessionVariable = new SessionVariable();
        HiveScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);

        // Create file cache values
        List<FileCacheValue> fileCaches = new ArrayList<>();
        FileCacheValue fileCache = new FileCacheValue();
        fileCache.setSplittable(true);
        RemoteFile file = new RemoteFile("file1", true, 1024, 1024);
        file.setPath(new Path("file1.parquet"));
        fileCache.addFile(file, new LocationPath("file1.parquet"));
        fileCaches.add(fileCache);

        // Create partition
        HivePartition partition = new HivePartition("testDb", "testTable", false,
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "hdfs://ns6666/user/hive/warehouse/test.db/test_table/a=dd",
                java.util.Arrays.asList("dd"), null);

        new Expectations() {
            {
                cache.getFilesByPartitions((List<HivePartition>) any, anyBoolean, anyString);
                result = fileCaches;
            }
        };

        List<Split> allFiles = new ArrayList<>();

        // Call the method that triggers line 437 with serde params
        scanNode.getFileSplitByPartitions(cache, java.util.Arrays.asList(partition), allFiles, "test", true);

        // Verify files were processed
        Assertions.assertFalse(allFiles.isEmpty(), "Should have generated file splits");
    }
}
