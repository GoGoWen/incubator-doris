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
import org.apache.doris.common.Config;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.FileScanNode;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.FileCacheValue;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;

import mockit.Expectations;
import mockit.Injectable;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HiveScanNodeTest {

    @Test
    public void testGetFileSplitSize(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog) {
        Config.max_selected_file_size_for_unrecommended_hive_table = 5000000000000L;
        Config.max_selected_total_file_size_for_hive_table = 60000000000000L;
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

                sessionVariable.getFileSplitSize();
                result = -1;
            }
        };
        Config.file_size_range_to_decide_split_size = new long[] {20 * 1024 * 1024 * 1024L, 40 * 1024 * 1024 * 1024L,
                80 * 1024 * 1024 * 1024L, 160 * 1024 * 1024 * 1024L, 320 * 1024 * 1024 * 1024L};
        HiveScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            context.resetTotalScanBytes();
            long fileSplitSize = scanNode.getFileSplitSize(fileCaches, false);
            Assertions.assertEquals(FileScanNode.DEFAULT_SPLIT_SIZE, fileSplitSize);
            Assertions.assertEquals(1024, context.getTotalScanBytes());
        } catch (Exception e) {
            Assertions.fail(e);
        }

        try {
            context.resetTotalScanBytes();
            long fileSplitSize = scanNode.getFileSplitSize(fileCaches, true);
            Assertions.assertEquals(FileScanNode.TINY_SPLIT_FILE_SIZE, fileSplitSize);
            Assertions.assertEquals(1024, context.getTotalScanBytes());
        } catch (Exception e) {
            Assertions.fail(e);
        }

        try {
            context.resetTotalScanBytes();
            fileCaches.get(0).getFiles().get(0).setLength(Config.file_size_range_to_decide_split_size[0] + 1);
            long fileSplitSize = scanNode.getFileSplitSize(fileCaches, true);
            Assertions.assertEquals(FileScanNode.SMALL_SPLIT_FILE_SIZE, fileSplitSize);
            Assertions.assertEquals(Config.file_size_range_to_decide_split_size[0] + 1, context.getTotalScanBytes());
        } catch (Exception e) {
            Assertions.fail(e);
        }

        try {
            context.resetTotalScanBytes();
            fileCaches.get(0).getFiles().get(0).setLength(Config.file_size_range_to_decide_split_size[1] + 1);
            long fileSplitSize = scanNode.getFileSplitSize(fileCaches, true);
            Assertions.assertEquals(FileScanNode.MEDIUM_SPLIT_FILE_SIZE, fileSplitSize);
            Assertions.assertEquals(Config.file_size_range_to_decide_split_size[1] + 1, context.getTotalScanBytes());
        } catch (Exception e) {
            Assertions.fail(e);
        }

        try {
            context.resetTotalScanBytes();
            fileCaches.get(0).getFiles().get(0).setLength(Config.file_size_range_to_decide_split_size[2] + 1);
            long fileSplitSize = scanNode.getFileSplitSize(fileCaches, true);
            Assertions.assertEquals(FileScanNode.LARGE_SPLIT_FILE_SIZE, fileSplitSize);
            Assertions.assertEquals(Config.file_size_range_to_decide_split_size[2] + 1, context.getTotalScanBytes());
        } catch (Exception e) {
            Assertions.fail(e);
        }

        try {
            context.resetTotalScanBytes();
            fileCaches.get(0).getFiles().get(0).setLength(Config.file_size_range_to_decide_split_size[3] + 1);
            long fileSplitSize = scanNode.getFileSplitSize(fileCaches, true);
            Assertions.assertEquals(FileScanNode.HUGE_SPLIT_FILE_SIZE, fileSplitSize);
            Assertions.assertEquals(Config.file_size_range_to_decide_split_size[3] + 1, context.getTotalScanBytes());
        } catch (Exception e) {
            Assertions.fail(e);
        }

        try {
            context.resetTotalScanBytes();
            fileCaches.get(0).getFiles().get(0).setLength(Config.file_size_range_to_decide_split_size[4] + 1);
            long fileSplitSize = scanNode.getFileSplitSize(fileCaches, true);
            Assertions.assertEquals(FileScanNode.DEFAULT_SPLIT_SIZE, fileSplitSize);
            Assertions.assertEquals(Config.file_size_range_to_decide_split_size[4] + 1, context.getTotalScanBytes());
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testNormalGenerateFileSplits(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog) throws IOException {
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
    public void testGetSelectedFileSize(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog) {
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
        Config.max_selected_file_size_for_unrecommended_hive_table = 5000;
        Config.max_selected_total_file_size_for_hive_table = 6000;
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
    public void testGenerateFileSplitsWithSortByFileSize(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog) throws IOException {
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
}
