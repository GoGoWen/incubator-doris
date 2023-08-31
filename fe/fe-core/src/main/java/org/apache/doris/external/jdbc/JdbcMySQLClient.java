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

package org.apache.doris.external.jdbc;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.Util;

import avro.shaded.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class JdbcMySQLClient extends JdbcClient {

    protected JdbcMySQLClient(String user, String password, String jdbcUrl, String driverUrl, String driverClass,
               String onlySpecifiedDatabase, String isLowerCaseTableNames) {
        super(user, password, jdbcUrl, driverUrl, driverClass, onlySpecifiedDatabase, isLowerCaseTableNames);
    }

    protected ResultSet getColumns(DatabaseMetaData databaseMetaData, String catalogName, String schemaName,
                                   String tableName) throws SQLException {
        return databaseMetaData.getColumns(schemaName, null, tableName, null);
    }

    /**
     * get all columns like DatabaseMetaData.getColumns in mysql-jdbc-connector
     */
    private Map<String, String> getJdbcColumnsTypeInfo(String dbName, String tableName) {
        Connection conn = getConnection();
        ResultSet resultSet = null;
        Map<String, String> fieldtoType = new HashMap<String, String>();

        StringBuilder queryBuf = new StringBuilder("SHOW FULL COLUMNS FROM ");
        queryBuf.append(tableName);
        queryBuf.append(" FROM ");
        queryBuf.append(dbName);
        try (Statement stmt = conn.createStatement()) {
            resultSet = stmt.executeQuery(queryBuf.toString());
            while (resultSet.next()) {
                // get column name
                String fieldName = resultSet.getString("Field");
                // get original type name
                String typeName = resultSet.getString("Type");
                fieldtoType.put(fieldName, typeName);
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get column list from jdbc for table %s:%s", tableName,
                Util.getRootCauseMessage(e));
        } finally {
            close(resultSet, conn);
        }

        return fieldtoType;
    }

    /**
     * get all columns of one table
     */
    @Override
    public List<JdbcFieldSchema> getJdbcColumnsInfo(String dbName, String tableName) {
        Connection conn = getConnection();
        ResultSet rs = null;
        List<JdbcFieldSchema> tableSchema = com.google.common.collect.Lists.newArrayList();
        // if isLowerCaseTableNames == true, tableName is lower case
        // but databaseMetaData.getColumns() is case sensitive
        if (isLowerCaseTableNames) {
            dbName = lowerDBToRealDB.get(dbName);
            tableName = lowerTableToRealTable.get(tableName);
        }
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalogName = conn.getCatalog();
            rs = getColumns(databaseMetaData, catalogName, dbName, tableName);
            boolean needGetDorisColumns = true;
            Map<String, String> mapFieldtoType = null;
            while (rs.next()) {
                JdbcFieldSchema field = new JdbcFieldSchema();
                field.setColumnName(rs.getString("COLUMN_NAME"));
                field.setDataType(rs.getInt("DATA_TYPE"));

                // in mysql-jdbc-connector-8.0.*, TYPE_NAME of the HLL column in doris will be "UNKNOWN"
                // in mysql-jdbc-connector-5.1.*, TYPE_NAME of the HLL column in doris will be "HLL"
                field.setDataTypeName(rs.getString("TYPE_NAME"));
                if (rs.getString("TYPE_NAME").equalsIgnoreCase("UNKNOWN")) {
                    if (needGetDorisColumns) {
                        mapFieldtoType = getJdbcColumnsTypeInfo(dbName, tableName);
                        needGetDorisColumns = false;
                    }

                    if (mapFieldtoType != null) {
                        field.setDataTypeName(mapFieldtoType.get(rs.getString("COLUMN_NAME")));
                    }
                }

                field.setColumnSize(rs.getInt("COLUMN_SIZE"));
                field.setDecimalDigits(rs.getInt("DECIMAL_DIGITS"));
                field.setNumPrecRadix(rs.getInt("NUM_PREC_RADIX"));
                /*
                   Whether it is allowed to be NULL
                   0 (columnNoNulls)
                   1 (columnNullable)
                   2 (columnNullableUnknown)
                 */
                field.setAllowNull(rs.getInt("NULLABLE") != 0);
                field.setRemarks(rs.getString("REMARKS"));
                field.setCharOctetLength(rs.getInt("CHAR_OCTET_LENGTH"));
                tableSchema.add(field);
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get table name list from jdbc for table %s:%s", e, tableName,
                Util.getRootCauseMessage(e));
        } finally {
            close(rs, conn);
        }
        return tableSchema;
    }
}
