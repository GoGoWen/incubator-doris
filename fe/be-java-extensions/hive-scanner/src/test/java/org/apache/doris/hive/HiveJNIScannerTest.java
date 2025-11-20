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

package org.apache.doris.hive;

import org.apache.doris.thrift.TFileFormatType;

import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Unit tests for HiveJNIScanner RCFile column projection fix.
 * Tests column ID parsing, full schema handling, and Hadoop property configuration.
 */
public class HiveJNIScannerTest {

    /**
     * Test that column_ids parameter is correctly parsed from BE.
     * This ensures schema positions are preserved for correct RCFile column projection.
     */
    @Test
    public void testColumnIdsParsingFromBE() throws Exception {
        // Given: BE sends column_ids representing schema positions
        Map<String, String> params = new HashMap<>();
        params.put(HiveProperties.FILE_TYPE, "0");
        params.put(HiveProperties.FILE_FORMAT, "17");  // FORMAT_RCBINARY
        params.put(HiveProperties.REQUIRED_FIELDS, "add_price,delivery_id,add_status");
        params.put(HiveProperties.COLUMNS_TYPES, "float#string#boolean");
        params.put(HiveProperties.COLUMN_IDS, "2,1,3");  // Schema positions
        params.put(HiveProperties.URI, "hdfs://test/path");
        params.put(HiveProperties.SPLIT_START_OFFSET, "0");
        params.put(HiveProperties.SPLIT_SIZE, "1024");

        // When: HiveJNIScanner is constructed
        HiveJNIScanner scanner = new HiveJNIScanner(100, params);

        // Then: requiredColumnIds should contain schema positions [2, 1, 3]
        int[] columnIds = getRequiredColumnIds(scanner);
        Assertions.assertNotNull(columnIds);
        Assertions.assertEquals(3, columnIds.length);
        Assertions.assertEquals(2, columnIds[0], "First field 'add_price' should map to schema position 2");
        Assertions.assertEquals(1, columnIds[1], "Second field 'delivery_id' should map to schema position 1");
        Assertions.assertEquals(3, columnIds[2], "Third field 'add_status' should map to schema position 3");
    }

    /**
     * Test fallback behavior when column_ids parameter is not provided by BE.
     * Should default to sequential indices [0, 1, 2, ...].
     */
    @Test
    public void testColumnIdsFallbackWhenNotProvided() throws Exception {
        // Given: column_ids parameter not provided (legacy behavior or non-Hive source)
        Map<String, String> params = new HashMap<>();
        params.put(HiveProperties.FILE_TYPE, "0");
        params.put(HiveProperties.FILE_FORMAT, "17");  // FORMAT_RCBINARY
        params.put(HiveProperties.REQUIRED_FIELDS, "col0,col1,col2");
        params.put(HiveProperties.COLUMNS_TYPES, "int#string#float");
        // Note: No COLUMN_IDS parameter
        params.put(HiveProperties.URI, "hdfs://test/path");
        params.put(HiveProperties.SPLIT_START_OFFSET, "0");
        params.put(HiveProperties.SPLIT_SIZE, "1024");

        // When: HiveJNIScanner is constructed
        HiveJNIScanner scanner = new HiveJNIScanner(100, params);

        // Then: requiredColumnIds should default to [0, 1, 2]
        int[] columnIds = getRequiredColumnIds(scanner);
        Assertions.assertNotNull(columnIds);
        Assertions.assertEquals(3, columnIds.length);
        Assertions.assertEquals(0, columnIds[0], "Should default to index 0");
        Assertions.assertEquals(1, columnIds[1], "Should default to index 1");
        Assertions.assertEquals(2, columnIds[2], "Should default to index 2");
    }

    /**
     * Test that createProperties() uses full schema when provided by FE.
     * This is critical for RCFile deserializer to correctly interpret column positions.
     *
     * Note: This test requires mocking Hadoop components and is commented out for now.
     * The functionality is covered by integration tests.
     */
    @Test
    public void testCreatePropertiesWithFullSchema() throws Exception {
        // Given: Full schema information provided by FE for RCFile
        Map<String, String> params = new HashMap<>();
        params.put(HiveProperties.FILE_TYPE, "0");
        params.put(HiveProperties.FILE_FORMAT, "17");  // FORMAT_RCBINARY
        params.put(HiveProperties.REQUIRED_FIELDS, "add_price,delivery_id");
        params.put(HiveProperties.COLUMNS_TYPES, "float#string");
        params.put(HiveProperties.COLUMN_IDS, "2,1");
        params.put(HiveProperties.FULL_SCHEMA_NAMES, "logging_time,delivery_id,add_price,add_status,dt");
        params.put(HiveProperties.FULL_SCHEMA_TYPES, "string,string,float,string,string");
        params.put(HiveProperties.URI, "hdfs://test/path");
        params.put(HiveProperties.SPLIT_START_OFFSET, "0");
        params.put(HiveProperties.SPLIT_SIZE, "1024");

        // When: HiveJNIScanner is constructed and createProperties is called
        HiveJNIScanner scanner = new HiveJNIScanner(100, params);
        Properties properties = invokeCreateProperties(scanner);

        // Then: Properties should contain FULL schema, not just projected columns
        String columns = properties.getProperty(HiveProperties.COLUMNS);
        String columnTypes = properties.getProperty(HiveProperties.COLUMNS2TYPES);

        Assertions.assertNotNull(columns, "COLUMNS property should be set");
        Assertions.assertNotNull(columnTypes, "COLUMNS.TYPES property should be set");

        // Verify full schema (5 columns)
        Assertions.assertEquals("logging_time,delivery_id,add_price,add_status,dt", columns,
                "Should use full schema with all 5 columns");
        Assertions.assertEquals("string,string,float,string,string", columnTypes,
                "Should use full schema types in Hive format");

        // Verify column projection configuration
        String readColumnIds = properties.getProperty(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR);
        String readColumnNames = properties.getProperty(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);

        Assertions.assertEquals("2,1", readColumnIds,
                "Should project only columns 2 and 1 (schema positions)");
        Assertions.assertEquals("add_price,delivery_id", readColumnNames,
                "Should project only required fields");
    }

    /**
     * Test fallback to projected schema when full schema is not provided.
     * This maintains compatibility with non-RCFile formats.
     *
     * Note: This test requires mocking Hadoop components and is commented out for now.
     */
    @Test
    public void testCreatePropertiesWithoutFullSchema() throws Exception {
        // Given: Full schema NOT provided (non-RCFile format or legacy code path)
        Map<String, String> params = new HashMap<>();
        params.put(HiveProperties.FILE_TYPE, "0");
        params.put(HiveProperties.FILE_FORMAT, "17");  // FORMAT_RCBINARY
        params.put(HiveProperties.REQUIRED_FIELDS, "add_price,delivery_id");
        params.put(HiveProperties.COLUMNS_TYPES, "float#string");
        params.put(HiveProperties.COLUMN_IDS, "2,1");
        // Note: No FULL_SCHEMA_NAMES or FULL_SCHEMA_TYPES
        params.put(HiveProperties.URI, "hdfs://test/path");
        params.put(HiveProperties.SPLIT_START_OFFSET, "0");
        params.put(HiveProperties.SPLIT_SIZE, "1024");

        // When: HiveJNIScanner is constructed and createProperties is called
        HiveJNIScanner scanner = new HiveJNIScanner(100, params);
        Properties properties = invokeCreateProperties(scanner);

        // Then: Properties should contain only projected schema (2 columns)
        String columns = properties.getProperty(HiveProperties.COLUMNS);
        String columnTypes = properties.getProperty(HiveProperties.COLUMNS2TYPES);

        Assertions.assertNotNull(columns, "COLUMNS property should be set");
        Assertions.assertNotNull(columnTypes, "COLUMNS.TYPES property should be set");

        // Verify projected schema only
        Assertions.assertEquals("add_price,delivery_id", columns,
                "Should fallback to projected schema (2 columns)");
        Assertions.assertEquals("float,string", columnTypes,
                "Should fallback to projected types");
    }

    /**
     * Test column projection configuration for complex query patterns.
     * Verifies non-sequential column selection works correctly.
     *
     * Note: This test requires mocking Hadoop components and is commented out for now.
     */
    @Test
    public void testColumnProjectionConfiguration() throws Exception {
        // Given: Query selects non-sequential columns (skip col0, select col2 then col1)
        Map<String, String> params = new HashMap<>();
        params.put(HiveProperties.FILE_TYPE, "0");
        params.put(HiveProperties.FILE_FORMAT, "17");  // FORMAT_RCBINARY
        params.put(HiveProperties.REQUIRED_FIELDS, "col2,col1");
        params.put(HiveProperties.COLUMNS_TYPES, "float#string");
        params.put(HiveProperties.COLUMN_IDS, "2,1");  // Non-sequential
        params.put(HiveProperties.FULL_SCHEMA_NAMES, "col0,col1,col2,col3");
        params.put(HiveProperties.FULL_SCHEMA_TYPES, "int,string,float,boolean");
        params.put(HiveProperties.URI, "hdfs://test/path");
        params.put(HiveProperties.SPLIT_START_OFFSET, "0");
        params.put(HiveProperties.SPLIT_SIZE, "1024");

        // When: Properties are created
        HiveJNIScanner scanner = new HiveJNIScanner(100, params);
        Properties properties = invokeCreateProperties(scanner);

        // Then: Verify all Hadoop configuration properties
        // 1. Column projection specifies which columns to read
        Assertions.assertEquals("2,1",
                properties.getProperty(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR),
                "Should read columns 2 and 1 (in that order)");
        Assertions.assertEquals("col2,col1",
                properties.getProperty(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR),
                "Should read col2 and col1 (in query order)");

        // 2. Full schema tells deserializer how to interpret positions
        Assertions.assertEquals("col0,col1,col2,col3",
                properties.getProperty(HiveProperties.COLUMNS),
                "Deserializer needs full schema to map positions");
        Assertions.assertEquals("int,string,float,boolean",
                properties.getProperty(HiveProperties.COLUMNS2TYPES),
                "Deserializer needs full type schema");

        // 3. Verify READ_ALL_COLUMNS is not set (we're using projection)
        Assertions.assertNull(properties.getProperty(ColumnProjectionUtils.READ_ALL_COLUMNS),
                "READ_ALL_COLUMNS should not be set when using column projection");
    }

    /**
     * Test edge case: single column selection.
     *
     * Note: This test requires mocking Hadoop components and is commented out for now.
     */
    @Test
    public void testSingleColumnProjection() throws Exception {
        // Given: Query selects only one column (not the first)
        Map<String, String> params = new HashMap<>();
        params.put(HiveProperties.FILE_TYPE, "0");
        params.put(HiveProperties.FILE_FORMAT, "17");  // FORMAT_RCBINARY
        params.put(HiveProperties.REQUIRED_FIELDS, "add_status");
        params.put(HiveProperties.COLUMNS_TYPES, "boolean");
        params.put(HiveProperties.COLUMN_IDS, "3");
        params.put(HiveProperties.FULL_SCHEMA_NAMES, "col0,col1,col2,add_status,col4");
        params.put(HiveProperties.FULL_SCHEMA_TYPES, "int,string,float,boolean,date");
        params.put(HiveProperties.URI, "hdfs://test/path");
        params.put(HiveProperties.SPLIT_START_OFFSET, "0");
        params.put(HiveProperties.SPLIT_SIZE, "1024");

        // When: Scanner is created
        HiveJNIScanner scanner = new HiveJNIScanner(100, params);
        int[] columnIds = getRequiredColumnIds(scanner);
        Properties properties = invokeCreateProperties(scanner);

        // Then: Single column projection should work
        Assertions.assertEquals(1, columnIds.length);
        Assertions.assertEquals(3, columnIds[0], "Should map to schema position 3");

        Assertions.assertEquals("3", properties.getProperty(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
        Assertions.assertEquals("add_status", properties.getProperty(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR));
        Assertions.assertEquals("col0,col1,col2,add_status,col4", properties.getProperty(HiveProperties.COLUMNS));
    }

    // Helper methods using reflection to access private fields/methods

    private int[] getRequiredColumnIds(HiveJNIScanner scanner) throws Exception {
        Field field = scanner.getClass().getDeclaredField("requiredColumnIds");
        field.setAccessible(true);
        return (int[]) field.get(scanner);
    }

    private Properties invokeCreateProperties(HiveJNIScanner scanner) throws Exception {
        // Initialize hiveFileContext which is normally done in initReader()
        Field hiveFileContextField = scanner.getClass().getDeclaredField("hiveFileContext");
        hiveFileContextField.setAccessible(true);
        Field fileFormatField = scanner.getClass().getDeclaredField("fileFormat");
        fileFormatField.setAccessible(true);
        TFileFormatType fileFormat = (TFileFormatType) fileFormatField.get(scanner);
        HiveFileContext hiveFileContext = new HiveFileContext(fileFormat);
        hiveFileContextField.set(scanner, hiveFileContext);

        Method method = scanner.getClass().getDeclaredMethod("createProperties");
        method.setAccessible(true);
        return (Properties) method.invoke(scanner);
    }
}
