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

import mockit.Expectations;
import mockit.Injectable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class MySQLJdbcExecutorTest {

    @Test
    public void testConvertTypeIfNecessary() {
        MySQLJdbcExecutor executor = new MySQLJdbcExecutor();
        String[] replaceStringList = new String[]{"bitmap", "hll", "int"};
        ColumnType columnType1 = new ColumnType("#bitmap", ColumnType.Type.INT);
        ColumnType columnType2 = new ColumnType("#hll", ColumnType.Type.INT);
        ColumnType columnType3 = new ColumnType("#int", ColumnType.Type.INT);
        ColumnType newColumnType1 = executor.convertTypeIfNecessary(0, columnType1, replaceStringList);
        ColumnType newColumnType2 = executor.convertTypeIfNecessary(1, columnType2, replaceStringList);
        ColumnType newColumnType3 = executor.convertTypeIfNecessary(2, columnType3, replaceStringList);

        ColumnType expectedColumnType1 = new ColumnType("#bitmap", ColumnType.Type.BYTE);
        ColumnType expectedColumnType2 = new ColumnType("#hll", ColumnType.Type.BYTE);
        ColumnType expectedColumnType3 = new ColumnType("#int", ColumnType.Type.INT);

        Assertions.assertEquals(expectedColumnType1.getName(), newColumnType1.getName());
        Assertions.assertEquals(expectedColumnType2.getName(), newColumnType2.getName());
        Assertions.assertEquals(expectedColumnType3.getName(), newColumnType3.getName());
        Assertions.assertEquals(expectedColumnType1.getType(), newColumnType1.getType());
        Assertions.assertEquals(expectedColumnType2.getType(), newColumnType2.getType());
        Assertions.assertEquals(expectedColumnType3.getType(), newColumnType3.getType());
    }

    @Test
    public void testGetColumnValue(@Injectable ResultSet mockResultSet) {
        MySQLJdbcExecutor executor = new MySQLJdbcExecutor();
        String[] replaceStringList = new String[]{"bitmap"};
        try {
            java.lang.reflect.Field resultSetField = executor.getClass().getSuperclass().getDeclaredField("resultSet");
            resultSetField.setAccessible(true);
            resultSetField.set(executor, mockResultSet);
        } catch (Exception e) {
            Assertions.fail("Failed to set resultSet field: " + e.getMessage());
        }
        try {
            new Expectations() {
                {
                    mockResultSet.getBytes(1);
                    result = new byte[]{1};
                    mockResultSet.wasNull();
                    result = false;
                }
            };
            Object result = executor.getColumnValue(0, new ColumnType("#bitmap", ColumnType.Type.BYTE),
                    replaceStringList);
            Assertions.assertTrue(result instanceof byte[]);
            Assertions.assertEquals(1, ((byte[]) (result))[0]);
        } catch (Exception e) {
            Assertions.fail(e);
        }
        try {
            new Expectations() {
                {
                    mockResultSet.getBytes(1);
                    result = null;
                    mockResultSet.wasNull();
                    result = true;
                }
            };
            Object result = executor.getColumnValue(0, new ColumnType("#hll", ColumnType.Type.BYTE),
                    replaceStringList);
            Assertions.assertNull(result);
        } catch (Exception e) {
            Assertions.fail(e);
        }
        LocalDate date = LocalDate.now();
        LocalDateTime dateTime = LocalDateTime.now();
        try {
            new Expectations() {
                {
                    mockResultSet.getObject(1, Boolean.class);
                    result = true;
                    mockResultSet.getObject(1);
                    result = 1;
                    mockResultSet.getObject(1, Integer.class);
                    result = 1;
                    mockResultSet.getObject(1, Long.class);
                    result = 125165613L;
                    mockResultSet.getObject(1, Float.class);
                    result = 1.3f;
                    mockResultSet.getObject(1, Double.class);
                    result = 2.1;
                    mockResultSet.getObject(1, BigDecimal.class);
                    result = new BigDecimal(1);
                    mockResultSet.getObject(1, LocalDate.class);
                    result = date;
                    mockResultSet.getObject(1, LocalDateTime.class);
                    result = dateTime;
                    mockResultSet.getObject(1, String.class);
                    result = "[1]";
                }
            };
            Object result1 = executor.getColumnValue(0, new ColumnType("#boolean", Type.BOOLEAN),
                    replaceStringList);
            Assertions.assertTrue(result1 instanceof Boolean);
            Assertions.assertTrue(((Boolean) result1).booleanValue());
            Object result2 = executor.getColumnValue(0, new ColumnType("#tinyint", Type.TINYINT),
                    replaceStringList);
            Assertions.assertEquals(1, ((Integer) result2).intValue());
            Object result3 = executor.getColumnValue(0, new ColumnType("#smallint", Type.SMALLINT),
                    replaceStringList);
            Assertions.assertEquals(1, ((Integer) result3).intValue());
            Object result4 = executor.getColumnValue(0, new ColumnType("#largeint", Type.LARGEINT),
                    replaceStringList);
            Assertions.assertEquals(1, ((Integer) result4).longValue());
            Object result5 = executor.getColumnValue(0, new ColumnType("#int", Type.INT),
                    replaceStringList);
            Assertions.assertEquals(1, ((Integer) result5).intValue());
            Object result6 = executor.getColumnValue(0, new ColumnType("#bigint", Type.BIGINT),
                    replaceStringList);
            Assertions.assertEquals(125165613L, ((Long) result6).longValue());
            Object result7 = executor.getColumnValue(0, new ColumnType("#float", Type.FLOAT),
                    replaceStringList);
            Assertions.assertEquals(1.3f, ((Float) result7).floatValue());
            Object result8 = executor.getColumnValue(0, new ColumnType("#double", Type.DOUBLE),
                    replaceStringList);
            Assertions.assertEquals(2.1, ((Double) result8).doubleValue());
            Object result9 = executor.getColumnValue(0, new ColumnType("#decimalv2", Type.DECIMALV2),
                    replaceStringList);
            Assertions.assertEquals(new BigDecimal(1), ((BigDecimal) result9));
            Object result10 = executor.getColumnValue(0, new ColumnType("#decimalv2", Type.DECIMALV2),
                    replaceStringList);
            Assertions.assertEquals(new BigDecimal(1), ((BigDecimal) result10));
            Object result11 = executor.getColumnValue(0, new ColumnType("#decimal32", Type.DECIMAL32),
                    replaceStringList);
            Assertions.assertEquals(new BigDecimal(1), ((BigDecimal) result11));
            Object result12 = executor.getColumnValue(0, new ColumnType("#decimal64", Type.DECIMAL64),
                    replaceStringList);
            Assertions.assertEquals(new BigDecimal(1), ((BigDecimal) result12));
            Object result13 = executor.getColumnValue(0, new ColumnType("#decimal128", Type.DECIMAL128),
                    replaceStringList);
            Assertions.assertEquals(new BigDecimal(1), ((BigDecimal) result13));
            Object result14 = executor.getColumnValue(0, new ColumnType("#date", Type.DATE),
                    replaceStringList);
            Assertions.assertEquals(date, ((LocalDate) result14));
            Object result15 = executor.getColumnValue(0, new ColumnType("#datev2", Type.DATEV2),
                    replaceStringList);
            Assertions.assertEquals(date, ((LocalDate) result15));
            Object result16 = executor.getColumnValue(0, new ColumnType("#datetime", Type.DATETIME),
                    replaceStringList);
            Assertions.assertEquals(dateTime, ((LocalDateTime) result16));
            Object result17 = executor.getColumnValue(0, new ColumnType("#datetimev2", Type.DATETIMEV2),
                    replaceStringList);
            Assertions.assertEquals(dateTime, ((LocalDateTime) result17));
            Object result18 = executor.getColumnValue(0, new ColumnType("#char", Type.CHAR),
                    replaceStringList);
            Assertions.assertEquals("[1]", ((String) result18));
            Object result19 = executor.getColumnValue(0, new ColumnType("#varchar", Type.VARCHAR),
                    replaceStringList);
            Assertions.assertEquals("[1]", ((String) result19));
            Object result20 = executor.getColumnValue(0, new ColumnType("#array", Type.ARRAY),
                    replaceStringList);
            Assertions.assertEquals("[1]", ((String) result20));
        } catch (Exception e) {
            Assertions.fail(e);
        }

        try {
            new Expectations() {
                {
                    mockResultSet.getObject(1);
                    result = "test";
                }
            };
            Object result21 = executor.getColumnValue(0, new ColumnType("#string", Type.STRING),
                    replaceStringList);
            Assertions.assertEquals("test", ((String) result21));
        } catch (Exception e) {
            Assertions.fail(e);
        }

    }
}
