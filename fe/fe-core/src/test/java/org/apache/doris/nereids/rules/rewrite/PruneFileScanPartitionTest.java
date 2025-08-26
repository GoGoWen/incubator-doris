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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.InternalQueryExecutionException;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Test for PruneFileScanPartition rule, focusing on executePartitionFilterQuery
 * method coverage.
 */
class PruneFileScanPartitionTest extends TestWithFeService implements MemoPatternMatchSupported {

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
                });

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
                });

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
                });

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

    @Test
    void testGetConjunctsWithoutPartitionPredicate(@Injectable LogicalFileScan logicalFileScan,
            @Injectable HMSExternalTable table) {
        Slot slot1 = new SlotReference("col1", IntegerType.INSTANCE);
        Slot slot2 = new SlotReference("col2", IntegerType.INSTANCE);
        Slot slot3 = new SlotReference("col3", StringType.INSTANCE);
        Slot slot4 = new SlotReference("col4", StringType.INSTANCE);

        Column col1 = new Column("col1", PrimitiveType.INT);
        Column col2 = new Column("col2", PrimitiveType.INT);

        Expression expression1 = new EqualTo(slot1, new IntegerLiteral(1));
        Expression expression2 = new LessThan(slot2, new IntegerLiteral(3));
        Expression expression3 = new EqualTo(slot3, new StringLiteral("abc"));

        new Expectations() {
            {
                logicalFileScan.getTable();
                result = table;
                minTimes = 1;

                logicalFileScan.getOutput();
                result = Lists.newArrayList(slot1, slot2, slot3, slot4);
                minTimes = 1;

                table.getPartitionColumns();
                result = Lists.newArrayList(col1, col2);
                minTimes = 1;
            }
        };
        LogicalFilter<LogicalFileScan> logicalFilter = new LogicalFilter<>(
                Sets.newHashSet(expression1, expression2, expression3), logicalFileScan);
        PruneFileScanPartition pruneFileScanPartition = new PruneFileScanPartition();
        Set<Expression> conjuncts = pruneFileScanPartition.getConjunctsWithoutPartitionPredicate(logicalFileScan,
                logicalFilter);
        Assertions.assertEquals(1, conjuncts.size());
        Assertions.assertEquals(expression3, conjuncts.toArray()[0]);
    }

    @Test
    void testPruneFilePartitionWithHudi(@Injectable HMSExternalTable table) {
        Slot slot1 = new SlotReference("col1", IntegerType.INSTANCE);
        Slot slot2 = new SlotReference("col2", IntegerType.INSTANCE);
        Slot slot3 = new SlotReference("col3", StringType.INSTANCE);

        Expression expression1 = new EqualTo(slot1, new IntegerLiteral(1));
        Expression expression2 = new LessThan(slot2, new IntegerLiteral(3));
        Expression expression3 = new EqualTo(slot3, new StringLiteral("abc"));

        final SelectedPartitions selectedPartitions = new SelectedPartitions(0, ImmutableMap.of(), true);
        Set<Expression> conjuncts = Sets.newHashSet(expression1, expression2, expression3);
        LogicalFileScan logicalFileScan = new LogicalFileScan(new RelationId(1), table,
                Lists.newArrayList("test", "test"), Optional.empty(), Optional.empty());
        new Expectations() {
            {
                table.getName();
                result = "test";
                minTimes = 1;

                table.getDlaType();
                result = DLAType.HUDI;
                minTimes = 1;
            }
        };
        new MockUp<PruneFileScanPartition>() {
            @Mock
            private SelectedPartitions pruneHivePartitions(HMSExternalTable hiveTbl,
                    LogicalFilter<LogicalFileScan> filter, LogicalFileScan scan, CascadesContext ctx,
                    boolean isViewBased) {
                return selectedPartitions;
            }

            @Mock
            Set<Expression> getConjunctsWithoutPartitionPredicate(LogicalFileScan fileScan,
                    LogicalFilter<LogicalFileScan> filter) {
                return Sets.newHashSet();
            }
        };
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(conjuncts, logicalFileScan);
        List<Plan> planList = new PruneFileScanPartition().build().transform(filter,
                MemoTestUtils.createCascadesContext(filter));
        Assertions.assertEquals(1, planList.size());
        Assertions.assertTrue(planList.get(0) instanceof LogicalFileScan);
        Assertions.assertEquals(selectedPartitions, ((LogicalFileScan) planList.get(0)).getSelectedPartitions());
    }

    @Test
    void testPruneFilePartitionWithHive(@Injectable HMSExternalTable table) {
        Slot slot1 = new SlotReference("col1", IntegerType.INSTANCE);
        Slot slot2 = new SlotReference("col2", IntegerType.INSTANCE);
        Slot slot3 = new SlotReference("col3", StringType.INSTANCE);

        Expression expression1 = new EqualTo(slot1, new IntegerLiteral(1));
        Expression expression2 = new LessThan(slot2, new IntegerLiteral(3));
        Expression expression3 = new EqualTo(slot3, new StringLiteral("abc"));

        final SelectedPartitions selectedPartitions = new SelectedPartitions(0, ImmutableMap.of(), true);
        Set<Expression> conjuncts = Sets.newHashSet(expression1, expression2, expression3);
        LogicalFileScan logicalFileScan = new LogicalFileScan(new RelationId(1), table,
                Lists.newArrayList("test", "test"), Optional.empty(), Optional.empty());
        new Expectations() {
            {
                table.getName();
                result = "test";
                minTimes = 1;

                table.getDlaType();
                result = DLAType.HIVE;
                minTimes = 1;
            }
        };
        new MockUp<PruneFileScanPartition>() {
            @Mock
            private SelectedPartitions pruneHivePartitions(HMSExternalTable hiveTbl,
                    LogicalFilter<LogicalFileScan> filter, LogicalFileScan scan, CascadesContext ctx,
                    boolean isViewBased) {
                return selectedPartitions;
            }

            @Mock
            Set<Expression> getConjunctsWithoutPartitionPredicate(LogicalFileScan fileScan,
                    LogicalFilter<LogicalFileScan> filter) {
                return Sets.newHashSet();
            }
        };
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(conjuncts, logicalFileScan);
        List<Plan> planList = new PruneFileScanPartition().build().transform(filter,
                MemoTestUtils.createCascadesContext(filter));
        Assertions.assertEquals(1, planList.size());
        Assertions.assertTrue(planList.get(0) instanceof LogicalFileScan);
        Assertions.assertTrue(((LogicalFileScan) planList.get(0)).getConjuncts().isEmpty());
    }
}
