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

package org.apache.doris.jdbc;

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnType.Type;
import org.apache.doris.common.jni.vec.VectorColumn;
import org.apache.doris.common.jni.vec.VectorTable;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class BaseJdbcExecutorTest {

    private TestableBaseJdbcExecutor executor;

    @BeforeEach
    public void setUp() {
        executor = new TestableBaseJdbcExecutor();
    }


    @Test
    public void testConvertTypeIfNecessary() {
        MySQLJdbcExecutor executor = new MySQLJdbcExecutor();
        String[] replaceStringList = new String[]{"int", "double", "string"};
        ColumnType columnType1 = new ColumnType("#int", ColumnType.Type.INT);
        ColumnType columnType2 = new ColumnType("#double", Type.DOUBLE);
        ColumnType columnType3 = new ColumnType("#string", Type.STRING);
        ColumnType newColumnType1 = executor.convertTypeIfNecessary(0, columnType1, replaceStringList);
        ColumnType newColumnType2 = executor.convertTypeIfNecessary(1, columnType2, replaceStringList);
        ColumnType newColumnType3 = executor.convertTypeIfNecessary(2, columnType3, replaceStringList);

        Assertions.assertEquals(columnType1.getName(), newColumnType1.getName());
        Assertions.assertEquals(columnType2.getName(), newColumnType2.getName());
        Assertions.assertEquals(columnType3.getName(), newColumnType3.getName());
        Assertions.assertEquals(columnType1.getType(), newColumnType1.getType());
        Assertions.assertEquals(columnType2.getType(), newColumnType2.getType());
        Assertions.assertEquals(columnType3.getType(), newColumnType3.getType());
    }

    @Test
    public void testGetBlockAddressWithValidParameters(@Mocked ResultSet mockResultSet,
                                                       @Mocked ResultSetMetaData mockMetaData,
                                                       @Mocked VectorTable mockOutputTable,
                                                       @Mocked VectorColumn mockColumn1,
                                                       @Mocked VectorColumn mockColumn2,
                                                       @Mocked VectorColumn mockColumn3) throws Exception {
        // Setup test data
        Map<String, String> outputParams = new HashMap<>();
        outputParams.put("is_nullable", "true,false,true");
        outputParams.put("replace_string", "string,int,string");

        // Mock VectorTable creation
        new MockUp<VectorTable>() {
            @Mock
            public VectorTable createWritableTable(Map<String, String> params, int reserved) {
                return mockOutputTable;
            }
        };

        // Setup expectations
        new Expectations() {
            {
                // Setup resultSet and metadata
                executor.setResultSet(mockResultSet);
                executor.setResultSetMetaData(mockMetaData);

                // Mock metadata for result set columns
                mockMetaData.getColumnCount();
                result = 3;

                mockMetaData.getColumnName(1);
                result = "NAME";
                mockMetaData.getColumnName(2);
                result = "AGE";
                mockMetaData.getColumnName(3);
                result = "EMAIL";

                // Mock output table structure
                mockOutputTable.getColumns();
                result = new VectorColumn[]{mockColumn1, mockColumn2, mockColumn3};

                mockOutputTable.getFields();
                result = new String[]{"name", "age", "email"};

                mockOutputTable.getColumnType(0);
                result = new ColumnType("name", ColumnType.Type.VARCHAR);
                mockOutputTable.getColumnType(1);
                result = new ColumnType("age", ColumnType.Type.INT);
                mockOutputTable.getColumnType(2);
                result = new ColumnType("email", ColumnType.Type.VARCHAR);

                // Mock result set data
                mockResultSet.next();
                returns(true, false); // First call returns true, second returns false

                mockOutputTable.getMetaAddress();
                result = 12345L;
            }
        };

        // Execute the method
        long result = executor.getBlockAddress(10, outputParams);

        // Verify result
        Assertions.assertEquals(12345L, result);
    }

    @Test
    public void testGetBlockAddressWithMissingParameters() {
        Map<String, String> outputParams = new HashMap<>();
        outputParams.put("is_nullable", "true,false");
        // Missing replace_string parameter

        JdbcExecutorException exception = Assertions.assertThrows(JdbcExecutorException.class,
                () -> executor.getBlockAddress(10, outputParams)
        );

        Assertions.assertNotNull(exception.getCause());
        Assertions.assertTrue(exception.getCause() instanceof NullPointerException);
    }

    @Test
    public void testGetBlockAddressWithColumnNotFound(@Mocked ResultSet mockResultSet,
                                                      @Mocked ResultSetMetaData mockMetaData,
                                                      @Mocked VectorTable mockOutputTable,
                                                      @Mocked VectorColumn mockColumn1,
                                                      @Mocked VectorColumn mockColumn2) throws Exception {
        Map<String, String> outputParams = new HashMap<>();
        outputParams.put("is_nullable", "true,false");
        outputParams.put("replace_string", "string,int");

        new MockUp<VectorTable>() {
            @Mock
            public VectorTable createWritableTable(Map<String, String> params, int reserved) {
                return mockOutputTable;
            }
        };

        new Expectations() {
            {
                executor.setResultSet(mockResultSet);
                executor.setResultSetMetaData(mockMetaData);

                // Mock metadata - result set has different column names
                mockMetaData.getColumnCount();
                result = 2;

                mockMetaData.getColumnName(1);
                result = "FIRST_NAME";  // Different from expected "name"
                mockMetaData.getColumnName(2);
                result = "USER_AGE";    // Different from expected "age"

                // Mock output table expecting different column names
                mockOutputTable.getColumns();
                result = new VectorColumn[]{mockColumn1, mockColumn2};

                mockOutputTable.getFields();
                result = new String[]{"name", "age"}; // These won't be found in result set
            }
        };

        // Should throw RuntimeException for column not found
        JdbcExecutorException exception = Assertions.assertThrows(JdbcExecutorException.class,
                () -> executor.getBlockAddress(10, outputParams)
        );

        Assertions.assertNotNull(exception.getCause());
        Assertions.assertTrue(exception.getCause() instanceof RuntimeException);
        Assertions.assertTrue(exception.getCause().getMessage().contains("Column not found"));
    }

    @Test
    public void testGetBlockAddressWithCaseInsensitiveColumnMatching(@Mocked ResultSet mockResultSet,
                                                                     @Mocked ResultSetMetaData mockMetaData,
                                                                     @Mocked VectorTable mockOutputTable,
                                                                     @Mocked VectorColumn mockColumn1,
                                                                     @Mocked VectorColumn mockColumn2) throws Exception {
        Map<String, String> outputParams = new HashMap<>();
        outputParams.put("is_nullable", "true,false");
        outputParams.put("replace_string", "string,int");

        new MockUp<VectorTable>() {
            @Mock
            public VectorTable createWritableTable(Map<String, String> params, int reserved) {
                return mockOutputTable;
            }
        };

        new Expectations() {
            {
                executor.setResultSet(mockResultSet);
                executor.setResultSetMetaData(mockMetaData);

                // Mock metadata with different case
                mockMetaData.getColumnCount();
                result = 2;

                mockMetaData.getColumnName(1);
                result = "NAME";  // Uppercase
                mockMetaData.getColumnName(2);
                result = "Age";   // Mixed case

                // Mock output table with lowercase names
                mockOutputTable.getColumns();
                result = new VectorColumn[]{mockColumn1, mockColumn2};

                mockOutputTable.getFields();
                result = new String[]{"name", "age"}; // lowercase - should still match

                mockOutputTable.getColumnType(0);
                result = new ColumnType("name", ColumnType.Type.VARCHAR);
                mockOutputTable.getColumnType(1);
                result = new ColumnType("age", ColumnType.Type.INT);

                mockResultSet.next();
                returns(true, false);

                mockOutputTable.getMetaAddress();
                result = 54321L;
            }
        };

        // Should work despite case differences
        long result = executor.getBlockAddress(10, outputParams);
        Assertions.assertEquals(54321L, result);
    }

    @Test
    public void testGetBlockAddressWithWhitespaceInColumnNames(@Mocked ResultSet mockResultSet,
                                                               @Mocked ResultSetMetaData mockMetaData,
                                                               @Mocked VectorTable mockOutputTable,
                                                               @Mocked VectorColumn mockColumn1) throws Exception {
        Map<String, String> outputParams = new HashMap<>();
        outputParams.put("is_nullable", "true");
        outputParams.put("replace_string", "string");

        new MockUp<VectorTable>() {
            @Mock
            public VectorTable createWritableTable(Map<String, String> params, int reserved) {
                return mockOutputTable;
            }
        };

        new Expectations() {
            {
                executor.setResultSet(mockResultSet);
                executor.setResultSetMetaData(mockMetaData);

                // Mock metadata with whitespace in column names
                mockMetaData.getColumnCount();
                result = 1;

                mockMetaData.getColumnName(1);
                result = "  USER_NAME  "; // Column name with leading/trailing whitespace

                // Mock output table - this demonstrates a bug where the original field name
                // is used in the lookup but the map was built with trimmed names
                mockOutputTable.getColumns();
                result = new VectorColumn[]{mockColumn1};

                mockOutputTable.getFields();
                result = new String[]{"  user_name  "}; // With whitespace - this will cause lookup failure
            }
        };

        // This should fail due to the whitespace bug in BaseJdbcExecutor
        JdbcExecutorException exception = Assertions.assertThrows(JdbcExecutorException.class,
                () -> executor.getBlockAddress(10, outputParams)
        );

        Assertions.assertNotNull(exception.getCause());
        Assertions.assertTrue(exception.getCause() instanceof RuntimeException);
        Assertions.assertTrue(exception.getCause().getMessage().contains("Column not found in result set"));
    }

    @Test
    public void testGetBlockAddressWithTrimmedColumnNames(@Mocked ResultSet mockResultSet,
                                                          @Mocked ResultSetMetaData mockMetaData,
                                                          @Mocked VectorTable mockOutputTable,
                                                          @Mocked VectorColumn mockColumn1) throws Exception {
        Map<String, String> outputParams = new HashMap<>();
        outputParams.put("is_nullable", "true");
        outputParams.put("replace_string", "string");

        new MockUp<VectorTable>() {
            @Mock
            public VectorTable createWritableTable(Map<String, String> params, int reserved) {
                return mockOutputTable;
            }
        };

        new Expectations() {
            {
                executor.setResultSet(mockResultSet);
                executor.setResultSetMetaData(mockMetaData);

                // Mock metadata with whitespace in column names
                mockMetaData.getColumnCount();
                result = 1;

                mockMetaData.getColumnName(1);
                result = "  USER_NAME  "; // Column name with leading/trailing whitespace

                // Mock output table with already trimmed names - this works correctly
                mockOutputTable.getColumns();
                result = new VectorColumn[]{mockColumn1};

                mockOutputTable.getFields();
                result = new String[]{"user_name"}; // Already trimmed - matches the map key

                mockOutputTable.getColumnType(0);
                result = new ColumnType("user_name", ColumnType.Type.VARCHAR);

                mockResultSet.next();
                returns(true, false);

                mockOutputTable.getMetaAddress();
                result = 88888L;
            }
        };

        // This should work because the field name matches the trimmed map key
        long result = executor.getBlockAddress(10, outputParams);
        Assertions.assertEquals(88888L, result);
    }

    @Test
    public void testGetBlockAddressWithMultipleBatches(@Mocked ResultSet mockResultSet,
                                                       @Mocked ResultSetMetaData mockMetaData,
                                                       @Mocked VectorTable mockOutputTable,
                                                       @Mocked VectorColumn mockColumn1) throws Exception {
        Map<String, String> outputParams = new HashMap<>();
        outputParams.put("is_nullable", "true");
        outputParams.put("replace_string", "string");

        new MockUp<VectorTable>() {
            @Mock
            public VectorTable createWritableTable(Map<String, String> params, int reserved) {
                return mockOutputTable;
            }
        };

        new Expectations() {
            {
                executor.setResultSet(mockResultSet);
                executor.setResultSetMetaData(mockMetaData);

                mockMetaData.getColumnCount();
                result = 1;

                mockMetaData.getColumnName(1);
                result = "name";

                mockOutputTable.getColumns();
                result = new VectorColumn[]{mockColumn1};

                mockOutputTable.getFields();
                result = new String[]{"name"};

                mockOutputTable.getColumnType(0);
                result = new ColumnType("name", ColumnType.Type.VARCHAR);

                // Mock multiple result set rows (more than batch size)
                mockResultSet.next();
                returns(true, true, true, false); // 3 rows, batch size is 2

                mockOutputTable.getMetaAddress();
                result = 77777L;
            }
        };

        // Test with batch size smaller than result set
        long result = executor.getBlockAddress(2, outputParams);
        Assertions.assertEquals(77777L, result);
    }

    @Test
    public void testGetBlockAddressWithSQLException(@Mocked ResultSet mockResultSet,
                                                    @Mocked ResultSetMetaData mockMetaData,
                                                    @Mocked VectorTable mockOutputTable) throws Exception {
        Map<String, String> outputParams = new HashMap<>();
        outputParams.put("is_nullable", "true");
        outputParams.put("replace_string", "string");

        new MockUp<VectorTable>() {
            @Mock
            public VectorTable createWritableTable(Map<String, String> params, int reserved) {
                return mockOutputTable;
            }
        };

        new Expectations() {
            {
                executor.setResultSet(mockResultSet);
                executor.setResultSetMetaData(mockMetaData);

                mockMetaData.getColumnCount();
                result = new SQLException("Database connection failed");
            }
        };

        // Should wrap SQLException in JdbcExecutorException
        JdbcExecutorException exception = Assertions.assertThrows(JdbcExecutorException.class,
                () -> executor.getBlockAddress(10, outputParams)
        );

        Assertions.assertTrue(exception.getCause() instanceof SQLException);
        Assertions.assertEquals("Database connection failed", exception.getCause().getMessage());
    }

    @Test
    public void testGetBlockAddressClosesExistingOutputTable(@Mocked VectorTable existingTable,
                                                             @Mocked VectorTable newTable,
                                                             @Mocked ResultSet mockResultSet,
                                                             @Mocked ResultSetMetaData mockMetaData,
                                                             @Mocked VectorColumn mockColumn1) throws Exception {
        Map<String, String> outputParams = new HashMap<>();
        outputParams.put("is_nullable", "true");
        outputParams.put("replace_string", "string");

        // Set existing output table
        executor.setOutputTable(existingTable);

        new MockUp<VectorTable>() {
            @Mock
            public VectorTable createWritableTable(Map<String, String> params, int reserved) {
                return newTable;
            }
        };

        new Expectations() {
            {
                executor.setResultSet(mockResultSet);
                executor.setResultSetMetaData(mockMetaData);

                // Verify existing table is closed
                existingTable.close();
                times = 1;

                mockMetaData.getColumnCount();
                result = 1;

                mockMetaData.getColumnName(1);
                result = "name";

                newTable.getColumns();
                result = new VectorColumn[]{mockColumn1};

                newTable.getFields();
                result = new String[]{"name"};

                newTable.getColumnType(0);
                result = new ColumnType("name", ColumnType.Type.VARCHAR);

                mockResultSet.next();
                returns(true, false);

                newTable.getMetaAddress();
                result = 11111L;
            }
        };

        long result = executor.getBlockAddress(10, outputParams);
        Assertions.assertEquals(11111L, result);
    }

    // Test helper class that extends BaseJdbcExecutor for testing
    private static class TestableBaseJdbcExecutor extends BaseJdbcExecutor {
        public TestableBaseJdbcExecutor() {
            // Initialize the block list
            this.block = new java.util.ArrayList<>();
        }

        public void setResultSet(ResultSet rs) {
            this.resultSet = rs;
        }

        public void setResultSetMetaData(ResultSetMetaData metaData) {
            this.resultSetMetaData = metaData;
        }

        public void setOutputTable(VectorTable table) {
            this.outputTable = table;
        }

        @Override
        protected void initializeBlock(int columnCount, String[] replaceStringList, int batchSizeNum,
                                       VectorTable outputTable) {
            // Simple implementation for testing
            for (int i = 0; i < columnCount; ++i) {
                block.add(new Object[batchSizeNum]);
            }
        }

        @Override
        protected Object getColumnValue(int columnIndex, ColumnType type, String[] replaceStringList) {
            // Simple mock implementation
            switch (type.getType()) {
                case VARCHAR:
                    return "test_value_" + columnIndex;
                case INT:
                    return columnIndex;
                default:
                    return null;
            }
        }

        @Override
        protected ColumnType convertTypeIfNecessary(int outputIdx, ColumnType origType, String[] replaceStringList) {
            return origType; // No conversion for testing
        }

        @Override
        protected org.apache.doris.common.jni.vec.ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
            return null; // Not needed for this test
        }
    }
}
