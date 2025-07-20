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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.InternalQueryExecutionException;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Test for PruneFileScanPartition rule, focusing on executePartitionFilterQuery method coverage.
 */
class PruneFileScanPartitionTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
    }

    /**
     * Test executePartitionFilterQuery method with successful execution
     */
    @Test
    void testExecutePartitionFilterQuerySuccess() throws Exception {
        List<ResultRow> mockResults = Lists.newArrayList();
        mockResults.add(new ResultRow(Lists.newArrayList("2023-01-01", "us")));
        mockResults.add(new ResultRow(Lists.newArrayList("2023-01-02", "eu")));

        new MockUp<StmtExecutor>() {
            @Mock
            public List<ResultRow> executeInternalQuery() throws Exception {
                return mockResults;
            }
        };

        HMSExternalTable mockTable = new HMSExternalTable(1L, "test_table", "test_db", null);
        List<ResultRow> result;
        try (AutoCloseConnectContext context = new AutoCloseConnectContext(connectContext)) {
            result = PruneFileScanPartition.executePartitionFilterQuery(mockTable, context, "SELECT * FROM test");
        }

        Assertions.assertNotNull(result, "Result should not be null");
        Assertions.assertEquals(2, result.size(), "Should return 2 partition rows");
        Assertions.assertEquals("2023-01-01", result.get(0).getValues().get(0),
                "First partition should have correct date");
        Assertions.assertEquals("us", result.get(0).getValues().get(1),
                "First partition should have correct region");
    }

    /**
     * Test executePartitionFilterQuery method exception handling
     */
    @Test
    void testExecutePartitionFilterQueryExceptionHandling() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public List<ResultRow> executeInternalQuery() throws Exception {
                throw new RuntimeException("Query execution failed");
            }
        };

        HMSExternalTable mockTable = new HMSExternalTable(1L, "test_table", "test_db", null);
        InternalQueryExecutionException exception = Assertions.assertThrows(
                InternalQueryExecutionException.class,
                () -> {
                    try (AutoCloseConnectContext context = new AutoCloseConnectContext(connectContext)) {
                        PruneFileScanPartition.executePartitionFilterQuery(mockTable, context, "SELECT * FROM test");
                    }
                }
        );

        String expectedMessage = "prune hive partitions failed for test_db.test_table";
        Assertions.assertEquals(expectedMessage, exception.getMessage(),
                "Exception message should match expected format");
    }

    /**
     * Test executePartitionFilterQuery with different exception types
     */
    @Test
    void testExecutePartitionFilterQueryDifferentExceptions() throws Exception {
        HMSExternalTable mockTable = new HMSExternalTable(1L, "test_table", "test_db", null);

        new MockUp<StmtExecutor>() {
            @Mock
            public List<ResultRow> executeInternalQuery() throws Exception {
                throw new java.sql.SQLException("Database connection failed");
            }
        };

        InternalQueryExecutionException sqlException = Assertions.assertThrows(
                InternalQueryExecutionException.class,
                () -> {
                    try (AutoCloseConnectContext context = new AutoCloseConnectContext(connectContext)) {
                        PruneFileScanPartition.executePartitionFilterQuery(mockTable, context, "SELECT * FROM test");
                    }
                }
        );

        Assertions.assertTrue(sqlException.getMessage().contains("prune hive partitions failed for"),
                "SQLException should be wrapped with standard error message");

        // Test with TimeoutException
        new MockUp<StmtExecutor>() {
            @Mock
            public List<ResultRow> executeInternalQuery() throws Exception {
                throw new java.util.concurrent.TimeoutException("Query timeout");
            }
        };

        InternalQueryExecutionException timeoutException = Assertions.assertThrows(
                InternalQueryExecutionException.class,
                () -> {
                    try (AutoCloseConnectContext context = new AutoCloseConnectContext(connectContext)) {
                        PruneFileScanPartition.executePartitionFilterQuery(mockTable, context, "SELECT * FROM test");
                    }
                }
        );

        Assertions.assertTrue(timeoutException.getMessage().contains("prune hive partitions failed for"),
                "TimeoutException should be wrapped with standard error message");
    }

    /**
     * Test executePartitionFilterQuery with empty result
     */
    @Test
    void testExecutePartitionFilterQueryEmptyResult() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public List<ResultRow> executeInternalQuery() throws Exception {
                return Lists.newArrayList(); // Empty result
            }
        };

        HMSExternalTable mockTable = new HMSExternalTable(1L, "test_table", "test_db", null);
        List<ResultRow> result;
        try (AutoCloseConnectContext context = new AutoCloseConnectContext(connectContext)) {
            result = PruneFileScanPartition.executePartitionFilterQuery(mockTable, context, "SELECT * FROM test");
        }

        Assertions.assertNotNull(result, "Result should not be null even when empty");
        Assertions.assertEquals(0, result.size(), "Should return empty list when no partitions match");
    }
}
