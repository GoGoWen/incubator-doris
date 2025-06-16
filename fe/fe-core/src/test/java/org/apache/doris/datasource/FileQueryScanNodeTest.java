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

package org.apache.doris.datasource;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.source.HiveScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.StatisticalType;

import mockit.Expectations;
import mockit.Injectable;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.lang.reflect.Field;

public class FileQueryScanNodeTest {
    @Test
    public void testGetNumInstances() {
        Config.enable_adaptive_generate_num_instances = true;
        Config.selected_split_num_to_decide_num_instances = new long[] {5000, 20000, 40000, 60000};
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(2));
        HMSExternalCatalog catalog = new HMSExternalCatalog();
        HMSExternalTable table = new HMSExternalTable(1, "test", "test", catalog);
        tuple.setTable(table);
        FileQueryScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tuple, "hive-scan-node",
                StatisticalType.HIVE_SCAN_NODE, true, connectContext.getSessionVariable());
        setSelectedSplitNumForFileQueryScanNode(scanNode, 2);
        Assertions.assertEquals(1, scanNode.getNumInstances());
        setSelectedSplitNumForFileQueryScanNode(scanNode, 6000);
        Assertions.assertEquals(2, scanNode.getNumInstances());
        setSelectedSplitNumForFileQueryScanNode(scanNode, 30000);
        Assertions.assertEquals(4, scanNode.getNumInstances());
        setSelectedSplitNumForFileQueryScanNode(scanNode, 50000);
        Assertions.assertEquals(8, scanNode.getNumInstances());
        setSelectedSplitNumForFileQueryScanNode(scanNode, 70000);
        Assertions.assertEquals(16, scanNode.getNumInstances());

        try {
            Config.enable_adaptive_generate_num_instances = false;
            connectContext.getSessionVariable().setPipelineTaskNum("8");
            Assertions.assertEquals(8, scanNode.getNumInstances());
        } catch (Exception e) {
            Assertions.fail(e);
        }

        ConnectContext.remove();
        Assertions.assertEquals(0, scanNode.getNumInstances());
    }

    private void setSelectedSplitNumForFileQueryScanNode(FileQueryScanNode scanNode, long selectedSplitNum) {
        try {
            Class<?> clazz = scanNode.getClass().getSuperclass();
            Field field = findFieldRecursively(clazz, "selectedSplitNum");
            field.setAccessible(true);
            field.set(scanNode, selectedSplitNum);
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    private Field findFieldRecursively(Class<?> clazz, String fieldName)
            throws NoSuchFieldException {
        Class<?> currentClass = clazz;
        while (currentClass != null) {
            try {
                return currentClass.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                currentClass = currentClass.getSuperclass();
            }
        }
        throw new NoSuchFieldException(
                String.format("filed '%s' not found", fieldName, clazz.getName())
        );
    }

    @Test
    public void testGetRealFileSplitSize(@Injectable SessionVariable sessionVariable,
                                         @Injectable TupleDescriptor tupleDesc,
                                         @Injectable HMSExternalTable table,
                                         @Injectable ExternalCatalog catalog) {
        new Expectations() {
            {
                sessionVariable.getFileSplitSize();
                result = 0;

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
        FileQueryScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);
        long splitSize1 = scanNode.getRealFileSplitSize(FileQueryScanNode.DEFAULT_SPLIT_SIZE);
        Assertions.assertEquals(FileQueryScanNode.DEFAULT_SPLIT_SIZE, splitSize1);
        new Expectations() {
            {
                sessionVariable.getFileSplitSize();
                result = 33554432;
            }
        };
        long splitSize2 = scanNode.getRealFileSplitSize(FileQueryScanNode.DEFAULT_SPLIT_SIZE);
        Assertions.assertEquals(33554432, splitSize2);
    }
}
