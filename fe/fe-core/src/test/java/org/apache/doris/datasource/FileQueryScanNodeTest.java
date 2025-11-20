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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.source.HiveScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.Multimap;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    @Test
    public void testGetScanRangeAssignment(@Injectable Multimap<Backend, Split> roundRobinAssignment,
            @Injectable Multimap<Backend, Split> defaultAssignment,
            @Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc,
            @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog,
            @Injectable Split split) throws UserException {
        new MockUp<EnhancedRoundRobinBackendPolicy>() {

            @Mock
            Multimap<Backend, Split> computeScanRangeAssignment(List<Split> splits) {
                return roundRobinAssignment;
            }

        };

        new MockUp<FederationBackendPolicy>() {
            @Mock
            Multimap<Backend, Split> computeScanRangeAssignment(List<Split> splits) {
                return defaultAssignment;
            }
        };

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
        FileQueryScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);
        Config.enable_enhanced_round_robin_backend_policy = false;
        List<Split> splits = new ArrayList<>();
        splits.add(split);
        Assert.assertEquals(defaultAssignment, scanNode.getScanRangeAssignment(splits));
        Config.enable_enhanced_round_robin_backend_policy = true;
        Assert.assertEquals(roundRobinAssignment, scanNode.getScanRangeAssignment(splits));
    }

    @Test
    public void testCreateFileRangeDesc(@Injectable Multimap<Backend, Split> roundRobinAssignment,
                                           @Injectable Multimap<Backend, Split> defaultAssignment,
                                           @Injectable SessionVariable sessionVariable,
                                           @Injectable TupleDescriptor tupleDesc,
                                           @Injectable HMSExternalTable table,
                                           @Injectable ExternalCatalog catalog,
                                           @Injectable Split split) throws UserException {
        new MockUp<EnhancedRoundRobinBackendPolicy>() {

            @Mock
            Multimap<Backend, Split> computeScanRangeAssignment(List<Split> splits) {
                return roundRobinAssignment;
            }

        };

        new MockUp<FederationBackendPolicy>() {
            @Mock
            Multimap<Backend, Split> computeScanRangeAssignment(List<Split> splits) {
                return defaultAssignment;
            }
        };

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
        FileQueryScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);
        FileSplit hdfsFileSplit = new FileSplit(new LocationPath(
                "hdfs://HDFSO1101/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e77f7d8.snappy.orc"),
                0, 112140970, 112140970, 0, null, Collections.emptyList());
        TFileRangeDesc rangeDesc = scanNode.createFileRangeDesc(hdfsFileSplit, Collections.emptyList(),
                Collections.emptyList());
        Assertions.assertEquals(0, rangeDesc.getStartOffset());
        Assertions.assertEquals(112140970, rangeDesc.getSize());
        Assertions.assertEquals(112140970, rangeDesc.getFileSize());
        Assertions.assertTrue(rangeDesc.getColumnsFromPath().isEmpty());
        Assertions.assertTrue(rangeDesc.getColumnsFromPathKeys().isEmpty());
        Assertions.assertEquals(TFileType.FILE_HDFS, rangeDesc.getFileType());
        Assertions.assertEquals("hdfs://HDFSO1101", rangeDesc.getFsName());
        Assertions.assertEquals(0, rangeDesc.getModificationTime());
    }

    @Test
    public void testToHiveTypeConversions(@Injectable SessionVariable sessionVariable,
                                          @Injectable TupleDescriptor tupleDesc,
                                          @Injectable HMSExternalTable table,
                                          @Injectable ExternalCatalog catalog) throws Exception {
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

        FileQueryScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable);
        Method toHiveTypeMethod = findMethodRecursively(scanNode.getClass().getSuperclass(), "toHiveType", Type.class);
        toHiveTypeMethod.setAccessible(true);

        // Test primitive types
        Assertions.assertEquals("boolean", toHiveTypeMethod.invoke(scanNode, Type.BOOLEAN));
        Assertions.assertEquals("tinyint", toHiveTypeMethod.invoke(scanNode, Type.TINYINT));
        Assertions.assertEquals("smallint", toHiveTypeMethod.invoke(scanNode, Type.SMALLINT));
        Assertions.assertEquals("int", toHiveTypeMethod.invoke(scanNode, Type.INT));
        Assertions.assertEquals("bigint", toHiveTypeMethod.invoke(scanNode, Type.BIGINT));
        Assertions.assertEquals("largeint", toHiveTypeMethod.invoke(scanNode, Type.LARGEINT));
        Assertions.assertEquals("float", toHiveTypeMethod.invoke(scanNode, Type.FLOAT));
        Assertions.assertEquals("double", toHiveTypeMethod.invoke(scanNode, Type.DOUBLE));

        // Test string types - all map to "string"
        Assertions.assertEquals("string", toHiveTypeMethod.invoke(scanNode, ScalarType.createCharType(10)));
        Assertions.assertEquals("string", toHiveTypeMethod.invoke(scanNode, ScalarType.createVarcharType(255)));
        Assertions.assertEquals("string", toHiveTypeMethod.invoke(scanNode, Type.STRING));

        // Test date/time types
        Assertions.assertEquals("date", toHiveTypeMethod.invoke(scanNode, Type.DATE));
        Assertions.assertEquals("date", toHiveTypeMethod.invoke(scanNode, Type.DATEV2));
        Assertions.assertEquals("timestamp", toHiveTypeMethod.invoke(scanNode, Type.DATETIME));
        Assertions.assertEquals("timestamp", toHiveTypeMethod.invoke(scanNode, Type.DATETIMEV2));

        // Test decimal types with precision/scale
        // ScalarType decimal1 = ScalarType.createDecimalV2Type(10, 2);
        // String decimalResult1 = (String) toHiveTypeMethod.invoke(scanNode, decimal1);
        // Assertions.assertTrue(decimalResult1.startsWith("decimal("));
        // Assertions.assertTrue(decimalResult1.contains("10"));
        // Assertions.assertTrue(decimalResult1.contains("2"));

        ScalarType decimal2 = ScalarType.createDecimalV3Type(20, 5);
        String decimalResult2 = (String) toHiveTypeMethod.invoke(scanNode, decimal2);
        Assertions.assertTrue(decimalResult2.startsWith("decimal("));
        Assertions.assertTrue(decimalResult2.contains("20"));
        Assertions.assertTrue(decimalResult2.contains("5"));
    }

    private Method findMethodRecursively(Class<?> clazz, String methodName, Class<?>... parameterTypes)
            throws NoSuchMethodException {
        Class<?> currentClass = clazz;
        while (currentClass != null) {
            try {
                return currentClass.getDeclaredMethod(methodName, parameterTypes);
            } catch (NoSuchMethodException e) {
                currentClass = currentClass.getSuperclass();
            }
        }
        throw new NoSuchMethodException(
                String.format("method '%s' not found in %s", methodName, clazz.getName())
        );
    }

    @Test
    public void testGenSlotToSchemaIdMapForRCBinary(@Injectable SessionVariable sessionVariable,
                                                     @Injectable TupleDescriptor tupleDesc,
                                                     @Injectable HMSExternalTable table,
                                                     @Injectable ExternalCatalog catalog) throws Exception {
        testGenSlotToSchemaIdMapForRCFile(sessionVariable, tupleDesc, table, catalog,
                org.apache.doris.thrift.TFileFormatType.FORMAT_RCBINARY);
    }

    @Test
    public void testGenSlotToSchemaIdMapForRCText(@Injectable SessionVariable sessionVariable,
                                                   @Injectable TupleDescriptor tupleDesc,
                                                   @Injectable HMSExternalTable table,
                                                   @Injectable ExternalCatalog catalog) throws Exception {
        testGenSlotToSchemaIdMapForRCFile(sessionVariable, tupleDesc, table, catalog,
                org.apache.doris.thrift.TFileFormatType.FORMAT_RCTEXT);
    }

    private void testGenSlotToSchemaIdMapForRCFile(SessionVariable sessionVariable,
                                                    TupleDescriptor tupleDesc,
                                                    HMSExternalTable table,
                                                    ExternalCatalog catalog,
                                                    org.apache.doris.thrift.TFileFormatType formatType) throws Exception {
        // Create base schema with multiple columns of different types
        List<org.apache.doris.catalog.Column> baseSchema = new ArrayList<>();
        baseSchema.add(new org.apache.doris.catalog.Column("id", Type.INT));
        baseSchema.add(new org.apache.doris.catalog.Column("name", Type.STRING));
        baseSchema.add(new org.apache.doris.catalog.Column("age", Type.TINYINT));
        baseSchema.add(new org.apache.doris.catalog.Column("salary", ScalarType.createDecimalV3Type(10, 2)));
        baseSchema.add(new org.apache.doris.catalog.Column("is_active", Type.BOOLEAN));

        // Create slot descriptors
        List<org.apache.doris.analysis.SlotDescriptor> slots = new ArrayList<>();
        for (int i = 0; i < baseSchema.size(); i++) {
            org.apache.doris.analysis.SlotDescriptor slot = new org.apache.doris.analysis.SlotDescriptor(
                    new org.apache.doris.analysis.SlotId(i), tupleDesc);
            slot.setColumn(baseSchema.get(i));
            slot.setIsMaterialized(true);
            slots.add(slot);
        }

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

                table.getBaseSchema();
                result = baseSchema;

                tupleDesc.getSlots();
                result = slots;
            }
        };

        // Create HiveScanNode with mocked getFileFormatType
        FileQueryScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable) {
            @Override
            public org.apache.doris.thrift.TFileFormatType getFileFormatType() throws UserException {
                return formatType;
            }
        };

        // Initialize params field
        Field paramsField = findFieldRecursively(scanNode.getClass().getSuperclass(), "params");
        paramsField.setAccessible(true);
        paramsField.set(scanNode, new org.apache.doris.thrift.TFileScanRangeParams());

        // Call genSlotToSchemaIdMapForOrc which handles RCFile format
        Method genSlotMethod = findMethodRecursively(scanNode.getClass().getSuperclass(), "genSlotToSchemaIdMapForOrc");
        genSlotMethod.setAccessible(true);
        genSlotMethod.invoke(scanNode);

        // Verify the properties were set correctly
        org.apache.doris.thrift.TFileScanRangeParams params =
                (org.apache.doris.thrift.TFileScanRangeParams) paramsField.get(scanNode);

        Assertions.assertNotNull(params.getProperties());
        Assertions.assertTrue(params.getProperties().containsKey("full_schema_names"));
        Assertions.assertTrue(params.getProperties().containsKey("full_schema_types"));

        // Verify full_schema_names
        String fullSchemaNames = params.getProperties().get("full_schema_names");
        Assertions.assertEquals("id,name,age,salary,is_active", fullSchemaNames);

        // Verify full_schema_types
        String fullSchemaTypes = params.getProperties().get("full_schema_types");
        Assertions.assertEquals("int,string,tinyint,decimal(10,2),boolean", fullSchemaTypes);
    }

    @Test
    public void testGenSlotToSchemaIdMapForRCFile_EmptySchema(@Injectable SessionVariable sessionVariable,
                                                               @Injectable TupleDescriptor tupleDesc,
                                                               @Injectable HMSExternalTable table,
                                                               @Injectable ExternalCatalog catalog) throws Exception {
        // Test with empty schema
        List<org.apache.doris.catalog.Column> baseSchema = new ArrayList<>();
        List<org.apache.doris.analysis.SlotDescriptor> slots = new ArrayList<>();

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

                table.getBaseSchema();
                result = baseSchema;

                tupleDesc.getSlots();
                result = slots;
            }
        };

        FileQueryScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable) {
            @Override
            public org.apache.doris.thrift.TFileFormatType getFileFormatType() throws UserException {
                return org.apache.doris.thrift.TFileFormatType.FORMAT_RCBINARY;
            }
        };

        Field paramsField = findFieldRecursively(scanNode.getClass().getSuperclass(), "params");
        paramsField.setAccessible(true);
        paramsField.set(scanNode, new org.apache.doris.thrift.TFileScanRangeParams());

        Method genSlotMethod = findMethodRecursively(scanNode.getClass().getSuperclass(), "genSlotToSchemaIdMapForOrc");
        genSlotMethod.setAccessible(true);
        genSlotMethod.invoke(scanNode);

        org.apache.doris.thrift.TFileScanRangeParams params =
                (org.apache.doris.thrift.TFileScanRangeParams) paramsField.get(scanNode);

        Assertions.assertNotNull(params.getProperties());
        Assertions.assertEquals("", params.getProperties().get("full_schema_names"));
        Assertions.assertEquals("", params.getProperties().get("full_schema_types"));
    }

    @Test
    public void testGenSlotToSchemaIdMapForRCFile_SingleColumn(@Injectable SessionVariable sessionVariable,
                                                                @Injectable TupleDescriptor tupleDesc,
                                                                @Injectable HMSExternalTable table,
                                                                @Injectable ExternalCatalog catalog) throws Exception {
        // Test with single column
        List<org.apache.doris.catalog.Column> baseSchema = new ArrayList<>();
        baseSchema.add(new org.apache.doris.catalog.Column("single_col", Type.BIGINT));

        List<org.apache.doris.analysis.SlotDescriptor> slots = new ArrayList<>();
        org.apache.doris.analysis.SlotDescriptor slot = new org.apache.doris.analysis.SlotDescriptor(
                new org.apache.doris.analysis.SlotId(0), tupleDesc);
        slot.setColumn(baseSchema.get(0));
        slot.setIsMaterialized(true);
        slots.add(slot);

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

                table.getBaseSchema();
                result = baseSchema;

                tupleDesc.getSlots();
                result = slots;
            }
        };

        FileQueryScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable) {
            @Override
            public org.apache.doris.thrift.TFileFormatType getFileFormatType() throws UserException {
                return org.apache.doris.thrift.TFileFormatType.FORMAT_RCTEXT;
            }
        };

        Field paramsField = findFieldRecursively(scanNode.getClass().getSuperclass(), "params");
        paramsField.setAccessible(true);
        paramsField.set(scanNode, new org.apache.doris.thrift.TFileScanRangeParams());

        Method genSlotMethod = findMethodRecursively(scanNode.getClass().getSuperclass(), "genSlotToSchemaIdMapForOrc");
        genSlotMethod.setAccessible(true);
        genSlotMethod.invoke(scanNode);

        org.apache.doris.thrift.TFileScanRangeParams params =
                (org.apache.doris.thrift.TFileScanRangeParams) paramsField.get(scanNode);

        Assertions.assertNotNull(params.getProperties());
        Assertions.assertEquals("single_col", params.getProperties().get("full_schema_names"));
        Assertions.assertEquals("bigint", params.getProperties().get("full_schema_types"));
    }

    @Test
    public void testGenSlotToSchemaIdMapForORC_NotRCFile(@Injectable SessionVariable sessionVariable,
                                                          @Injectable TupleDescriptor tupleDesc,
                                                          @Injectable HMSExternalTable table,
                                                          @Injectable ExternalCatalog catalog) throws Exception {
        // Test that ORC format doesn't add RCFile properties
        List<org.apache.doris.catalog.Column> baseSchema = new ArrayList<>();
        baseSchema.add(new org.apache.doris.catalog.Column("id", Type.INT));
        List<org.apache.doris.analysis.SlotDescriptor> slots = new ArrayList<>();
        org.apache.doris.analysis.SlotDescriptor slot = new org.apache.doris.analysis.SlotDescriptor(
                new org.apache.doris.analysis.SlotId(0), tupleDesc);
        slot.setColumn(baseSchema.get(0));
        slot.setIsMaterialized(true);
        slots.add(slot);

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

                table.getBaseSchema();
                result = baseSchema;

                tupleDesc.getSlots();
                result = slots;
            }
        };

        FileQueryScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable) {
            @Override
            public org.apache.doris.thrift.TFileFormatType getFileFormatType() throws UserException {
                return org.apache.doris.thrift.TFileFormatType.FORMAT_ORC;
            }
        };

        Field paramsField = findFieldRecursively(scanNode.getClass().getSuperclass(), "params");
        paramsField.setAccessible(true);
        paramsField.set(scanNode, new org.apache.doris.thrift.TFileScanRangeParams());

        Method genSlotMethod = findMethodRecursively(scanNode.getClass().getSuperclass(), "genSlotToSchemaIdMapForOrc");
        genSlotMethod.setAccessible(true);
        genSlotMethod.invoke(scanNode);

        org.apache.doris.thrift.TFileScanRangeParams params =
                (org.apache.doris.thrift.TFileScanRangeParams) paramsField.get(scanNode);

        // ORC format should not add full_schema_names/types properties
        if (params.getProperties() != null) {
            Assertions.assertFalse(params.getProperties().containsKey("full_schema_names"));
            Assertions.assertFalse(params.getProperties().containsKey("full_schema_types"));
        }
    }

    @Test
    public void testGenSlotToSchemaIdMapForRCFile_WithExistingProperties(@Injectable SessionVariable sessionVariable,
                                                                          @Injectable TupleDescriptor tupleDesc,
                                                                          @Injectable HMSExternalTable table,
                                                                          @Injectable ExternalCatalog catalog) throws Exception {
        // Test that existing properties are preserved
        List<org.apache.doris.catalog.Column> baseSchema = new ArrayList<>();
        baseSchema.add(new org.apache.doris.catalog.Column("col1", Type.INT));
        baseSchema.add(new org.apache.doris.catalog.Column("col2", Type.STRING));

        List<org.apache.doris.analysis.SlotDescriptor> slots = new ArrayList<>();
        for (int i = 0; i < baseSchema.size(); i++) {
            org.apache.doris.analysis.SlotDescriptor slot = new org.apache.doris.analysis.SlotDescriptor(
                    new org.apache.doris.analysis.SlotId(i), tupleDesc);
            slot.setColumn(baseSchema.get(i));
            slot.setIsMaterialized(true);
            slots.add(slot);
        }

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

                table.getBaseSchema();
                result = baseSchema;

                tupleDesc.getSlots();
                result = slots;
            }
        };

        FileQueryScanNode scanNode = new HiveScanNode(new PlanNodeId(1), tupleDesc, true, sessionVariable) {
            @Override
            public org.apache.doris.thrift.TFileFormatType getFileFormatType() throws UserException {
                return org.apache.doris.thrift.TFileFormatType.FORMAT_RCBINARY;
            }
        };

        Field paramsField = findFieldRecursively(scanNode.getClass().getSuperclass(), "params");
        paramsField.setAccessible(true);
        org.apache.doris.thrift.TFileScanRangeParams params = new org.apache.doris.thrift.TFileScanRangeParams();

        // Set existing properties
        java.util.Map<String, String> existingProps = new java.util.HashMap<>();
        existingProps.put("existing_key", "existing_value");
        params.setProperties(existingProps);
        paramsField.set(scanNode, params);

        Method genSlotMethod = findMethodRecursively(scanNode.getClass().getSuperclass(), "genSlotToSchemaIdMapForOrc");
        genSlotMethod.setAccessible(true);
        genSlotMethod.invoke(scanNode);

        params = (org.apache.doris.thrift.TFileScanRangeParams) paramsField.get(scanNode);

        Assertions.assertNotNull(params.getProperties());
        // Existing property should be preserved
        Assertions.assertEquals("existing_value", params.getProperties().get("existing_key"));
        // New properties should be added
        Assertions.assertEquals("col1,col2", params.getProperties().get("full_schema_names"));
        Assertions.assertEquals("int,string", params.getProperties().get("full_schema_types"));
    }
}
