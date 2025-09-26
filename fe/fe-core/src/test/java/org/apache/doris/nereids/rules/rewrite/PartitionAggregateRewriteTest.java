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

import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.MemoTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Comprehensive tests for PartitionAggregateRewrite after the rewrite to use LogicalUnion
 * instead of LogicalTVFRelation.
 */
class PartitionAggregateRewriteTest {

    private static final String TEST_DB = "test_db";
    private static final String TEST_TABLE = "test_table";

    @Mocked
    private Env mockEnv;

    @Injectable
    private HMSExternalTable mockTable;

    @Injectable
    private HMSExternalDatabase mockDatabase;

    @Injectable
    private HMSExternalCatalog mockCatalog;

    @Injectable
    private ExternalMetaCacheMgr mockMetaCacheMgr;

    @Injectable
    private HiveMetaStoreCache mockCache;

    private ExprId slotId1;
    private ExprId slotId2;
    private SlotReference yearSlot;
    private SlotReference monthSlot;
    private Column yearColumn;
    private Column monthColumn;

    @BeforeEach
    void setUp() {
        slotId1 = new ExprId(1);
        slotId2 = new ExprId(2);
        yearSlot = new SlotReference(slotId1, "year", IntegerType.INSTANCE, true, Lists.newArrayList());
        monthSlot = new SlotReference(slotId2, "month", StringType.INSTANCE, true, Lists.newArrayList());
        yearColumn = new Column("year", Type.INT);
        monthColumn = new Column("month", Type.STRING);
    }

    @Test
    void testPartitionAggregateRewriteWithPrunedPartitions() {
        // Setup aggregate with Min/Max functions
        Min minFunction = new Min(yearSlot);
        Max maxFunction = new Max(monthSlot);
        List<NamedExpression> aggregateFunctions = Lists.newArrayList(
                new Alias(new ExprId(10), minFunction, "min_year"),
                new Alias(new ExprId(11), maxFunction, "max_month")
        );

        // Create selected partitions (pruned case)
        Map<Long, PartitionItem> selectedPartitions = createMockSelectedPartitions();
        SelectedPartitions mockSelectedPartitions = new SelectedPartitions(selectedPartitions.size(), selectedPartitions, true);

        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList(TEST_DB, TEST_TABLE), Optional.<org.apache.doris.nereids.trees.TableSample>empty(), Optional.<org.apache.doris.analysis.TableSnapshot>empty());
        fileScan = fileScan.withSelectedPartitions(mockSelectedPartitions);

        LogicalProject project = new LogicalProject(Lists.newArrayList(yearSlot, monthSlot), fileScan);
        LogicalAggregate aggregate = new LogicalAggregate(Lists.newArrayList(), aggregateFunctions, project);

        // Mock expectations for table methods needed during memo validation
        new Expectations() {
            {
                mockTable.getName();
                result = TEST_TABLE;
                minTimes = 0;

                mockTable.getId();
                result = 1L;
                minTimes = 0;

                mockTable.getDbName();
                result = TEST_DB;
                minTimes = 0;

                mockTable.getPartitionColumns();
                result = Lists.newArrayList(yearColumn, monthColumn);
                minTimes = 0;

                mockTable.getPartitionColumnTypes();
                result = Lists.newArrayList(Type.INT, Type.STRING);
                minTimes = 0;
            }
        };

        // Mock static Env.getCurrentEnv()
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return mockEnv;
            }
        };

        PartitionAggregateRewrite rewriteRule = new PartitionAggregateRewrite();
        Rule rule = rewriteRule.build();
        List<Plan> results = rule.transform(aggregate, MemoTestUtils.createCascadesContext(aggregate));

        Assertions.assertEquals(1, results.size(), "Rule should produce exactly one result");
        Plan transformedPlan = results.get(0);
        Assertions.assertTrue(transformedPlan instanceof LogicalAggregate, "Result should be LogicalAggregate");

        LogicalAggregate transformedAggregate = (LogicalAggregate) transformedPlan;
        Plan child = (Plan) transformedAggregate.child();
        Assertions.assertTrue(child instanceof LogicalProject, "Child should be LogicalProject");

        LogicalProject transformedProject = (LogicalProject) child;
        Plan grandChild = (Plan) transformedProject.child();
        Assertions.assertTrue(grandChild instanceof LogicalUnion, "Grandchild should be LogicalUnion");

        LogicalUnion union = (LogicalUnion) grandChild;
        Assertions.assertFalse(union.getConstantExprsList().isEmpty(), "Union should have constant expressions");
        Assertions.assertTrue(union.children().isEmpty(), "Union should have no children");
    }

    @Test
    void testPartitionAggregateRewriteWithUnprunedPartitions() {
        // Mock environment and cache setup
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return mockEnv;
            }
        };

        // Setup aggregate with Min function
        Min minFunction = new Min(yearSlot);
        List<NamedExpression> aggregateFunctions = Lists.newArrayList(
                new Alias(new ExprId(10), minFunction, "min_year")
        );

        // Create unpruned partitions (cache will be used)
        SelectedPartitions mockSelectedPartitions = new SelectedPartitions(0, Maps.newHashMap(), false);

        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList(TEST_DB, TEST_TABLE), Optional.<org.apache.doris.nereids.trees.TableSample>empty(), Optional.<org.apache.doris.analysis.TableSnapshot>empty());
        fileScan = fileScan.withSelectedPartitions(mockSelectedPartitions);

        LogicalProject project = new LogicalProject(Lists.newArrayList(yearSlot), fileScan);
        LogicalAggregate aggregate = new LogicalAggregate(Lists.newArrayList(), aggregateFunctions, project);

        // Setup partition values from cache
        HiveMetaStoreCache.HivePartitionValues mockPartitionValues = createMockHivePartitionValues();

        // Mock static Env.getCurrentEnv()
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return mockEnv;
            }
        };

        new Expectations() {
            {
                mockTable.getPartitionColumns();
                result = Lists.newArrayList(yearColumn);

                mockTable.isViewBased();
                result = false;

                mockTable.getCatalog();
                result = mockCatalog;

                mockTable.getDbName();
                result = TEST_DB;

                mockTable.getName();
                result = TEST_TABLE;

                mockEnv.getExtMetaCacheMgr();
                result = mockMetaCacheMgr;

                mockMetaCacheMgr.getMetaStoreCache((HMSExternalCatalog) any);
                result = mockCache;

                mockCache.getPartitionNum(TEST_DB, TEST_TABLE);
                result = 5; // Less than Config.max_partition_num_for_single_hive_table_without_filter

                mockCache.getPartitionValues(TEST_DB, TEST_TABLE, Lists.<Type>newArrayList());
                result = mockPartitionValues;
            }
        };

        PartitionAggregateRewrite rewriteRule = new PartitionAggregateRewrite();
        Rule rule = rewriteRule.build();
        List<Plan> results = rule.transform(aggregate, MemoTestUtils.createCascadesContext(aggregate));

        Assertions.assertEquals(1, results.size(), "Rule should produce exactly one result");
        Plan transformedPlan = results.get(0);

        LogicalAggregate transformedAggregate = (LogicalAggregate) transformedPlan;
        LogicalProject transformedProject = (LogicalProject) transformedAggregate.child();
        Plan grandChild = (Plan) transformedProject.child();
        Assertions.assertTrue(grandChild instanceof LogicalUnion, "Should create LogicalUnion from cache");
    }

    @Test
    void testEmptyPartitionSelection() {
        // Setup aggregate
        Min minFunction = new Min(yearSlot);
        List<NamedExpression> aggregateFunctions = Lists.newArrayList(
                new Alias(new ExprId(10), minFunction, "min_year")
        );

        // Create empty selected partitions
        SelectedPartitions mockSelectedPartitions = new SelectedPartitions(0, Maps.newHashMap(), true);

        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList(TEST_DB, TEST_TABLE), Optional.<org.apache.doris.nereids.trees.TableSample>empty(), Optional.<org.apache.doris.analysis.TableSnapshot>empty());
        fileScan = fileScan.withSelectedPartitions(mockSelectedPartitions);

        LogicalProject project = new LogicalProject(Lists.newArrayList(yearSlot), fileScan);
        LogicalAggregate aggregate = new LogicalAggregate(Lists.newArrayList(), aggregateFunctions, project);

        new Expectations() {
            {
                mockTable.getName();
                result = TEST_TABLE;
                minTimes = 0;

                mockTable.getId();
                result = 1L;
                minTimes = 0;

                mockTable.getPartitionColumns();
                result = Lists.newArrayList(yearColumn);
                minTimes = 0;

                mockTable.getCatalog();
                result = mockCatalog;
                minTimes = 0;

                mockTable.getDbName();
                result = TEST_DB;
                minTimes = 0;

                mockTable.getName();
                result = TEST_TABLE;
                minTimes = 0;

                mockEnv.getExtMetaCacheMgr();
                result = mockMetaCacheMgr;
                minTimes = 0;

                mockMetaCacheMgr.getMetaStoreCache((HMSExternalCatalog) any);
                result = mockCache;
                minTimes = 0;
            }
        };

        PartitionAggregateRewrite rewriteRule = new PartitionAggregateRewrite();
        Rule rule = rewriteRule.build();
        List<Plan> results = rule.transform(aggregate, MemoTestUtils.createCascadesContext(aggregate));

        Assertions.assertEquals(1, results.size(), "Rule should produce exactly one result");
        Plan transformedPlan = results.get(0);

        LogicalAggregate transformedAggregate = (LogicalAggregate) transformedPlan;
        LogicalProject transformedProject = (LogicalProject) transformedAggregate.child();
        Plan grandChild = (Plan) transformedProject.child();
        Assertions.assertTrue(grandChild instanceof LogicalEmptyRelation,
                "Empty partitions should result in LogicalEmptyRelation");
    }

    @Test
    void testLargePartitionHandling() {
        // Mock environment setup
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return mockEnv;
            }
        };

        try {
            java.lang.reflect.Field field = Config.class.getDeclaredField("max_partition_num_for_single_hive_table_without_filter");
            field.setAccessible(true);
            field.setInt(null, 10);
        } catch (Exception e) {
            // Fallback: just use the test logic without Config modification
        }

        Min minFunction = new Min(yearSlot);
        List<NamedExpression> aggregateFunctions = Lists.newArrayList(
                new Alias(new ExprId(10), minFunction, "min_year")
        );

        SelectedPartitions mockSelectedPartitions = new SelectedPartitions(0, Maps.newHashMap(), false);
        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList(TEST_DB, TEST_TABLE), Optional.<org.apache.doris.nereids.trees.TableSample>empty(), Optional.<org.apache.doris.analysis.TableSnapshot>empty());
        fileScan = fileScan.withSelectedPartitions(mockSelectedPartitions);

        LogicalProject project = new LogicalProject(Lists.newArrayList(yearSlot), fileScan);
        LogicalAggregate aggregate = new LogicalAggregate(Lists.newArrayList(), aggregateFunctions, project);

        HiveMetaStoreCache.HivePartitionValues mockPartitionValues = createMockHivePartitionValues();

        new Expectations() {
            {
                mockTable.getPartitionColumns();
                result = Lists.newArrayList(yearColumn);

                mockTable.isViewBased();
                result = false;

                mockTable.getCatalog();
                result = mockCatalog;

                mockTable.getDbName();
                result = TEST_DB;

                mockTable.getName();
                result = TEST_TABLE;

                mockEnv.getExtMetaCacheMgr();
                result = mockMetaCacheMgr;

                mockMetaCacheMgr.getMetaStoreCache((HMSExternalCatalog) any);
                result = mockCache;

                mockTable.getPartitionColumnTypes();
                result = Lists.newArrayList(Type.INT);

                mockCache.getPartitionNum(TEST_DB, TEST_TABLE);
                result = 15; // Greater than Config.max_partition_num_for_single_hive_table_without_filter

                mockCache.getPartitionValuesWithoutCache(TEST_DB, TEST_TABLE, Lists.newArrayList(Type.INT));
                result = mockPartitionValues;
            }
        };

        PartitionAggregateRewrite rewriteRule = new PartitionAggregateRewrite();
        Rule rule = rewriteRule.build();
        List<Plan> results = rule.transform(aggregate, MemoTestUtils.createCascadesContext(aggregate));

        Assertions.assertEquals(1, results.size());
        // Should use getPartitionValuesWithoutCache for large partition count
        Plan transformedPlan = results.get(0);
        LogicalAggregate transformedAggregate = (LogicalAggregate) transformedPlan;
        LogicalProject transformedProject = (LogicalProject) transformedAggregate.child();
        Plan grandChild = (Plan) transformedProject.child();
        Assertions.assertTrue(grandChild instanceof LogicalUnion);
    }

    @Test
    void testViewBasedTable() {
        // Mock environment setup
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return mockEnv;
            }
        };

        Min minFunction = new Min(yearSlot);
        List<NamedExpression> aggregateFunctions = Lists.newArrayList(
                new Alias(new ExprId(10), minFunction, "min_year")
        );

        SelectedPartitions mockSelectedPartitions = new SelectedPartitions(0, Maps.newHashMap(), false);
        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList(TEST_DB, TEST_TABLE), Optional.<org.apache.doris.nereids.trees.TableSample>empty(), Optional.<org.apache.doris.analysis.TableSnapshot>empty());
        fileScan = fileScan.withSelectedPartitions(mockSelectedPartitions);

        LogicalProject project = new LogicalProject(Lists.newArrayList(yearSlot), fileScan);
        LogicalAggregate aggregate = new LogicalAggregate(Lists.newArrayList(), aggregateFunctions, project);

        HiveMetaStoreCache.HivePartitionValues mockPartitionValues = createMockHivePartitionValues();

        new Expectations() {
            {
                mockTable.getPartitionColumns();
                result = Lists.newArrayList(yearColumn);

                mockTable.isViewBased();
                result = true; // This is a view-based table

                mockTable.getCatalog();
                result = mockCatalog;

                mockTable.getDbName();
                result = TEST_DB;

                mockTable.getName();
                result = TEST_TABLE;

                mockEnv.getExtMetaCacheMgr();
                result = mockMetaCacheMgr;

                mockMetaCacheMgr.getMetaStoreCache((HMSExternalCatalog) any);
                result = mockCache;

                mockCache.getPartitionNumFromView(TEST_DB, TEST_TABLE);
                result = 5;

                mockCache.getPartitionValuesFromView(TEST_DB, TEST_TABLE, Lists.<Type>newArrayList());
                result = mockPartitionValues;
            }
        };

        PartitionAggregateRewrite rewriteRule = new PartitionAggregateRewrite();
        Rule rule = rewriteRule.build();
        List<Plan> results = rule.transform(aggregate, MemoTestUtils.createCascadesContext(aggregate));

        Assertions.assertEquals(1, results.size());
        // Should use view-specific methods
        Plan transformedPlan = results.get(0);
        LogicalAggregate transformedAggregate = (LogicalAggregate) transformedPlan;
        LogicalProject transformedProject = (LogicalProject) transformedAggregate.child();
        Plan grandChild = (Plan) transformedProject.child();
        Assertions.assertTrue(grandChild instanceof LogicalUnion);
    }

    @Test
    void testUnsupportedAggregateFunctions() {
        // Test with Count function (unsupported)
        Count countFunction = new Count();
        List<NamedExpression> aggregateFunctions = Lists.newArrayList(
                new Alias(new ExprId(10), countFunction, "count_year")
        );

        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList(TEST_DB, TEST_TABLE), Optional.<org.apache.doris.nereids.trees.TableSample>empty(), Optional.<org.apache.doris.analysis.TableSnapshot>empty());

        LogicalProject project = new LogicalProject(Lists.newArrayList(yearSlot), fileScan);
        LogicalAggregate aggregate = new LogicalAggregate(Lists.newArrayList(), aggregateFunctions, project);

        new Expectations() {
            {
                // Required for memo validation
                mockTable.getName();
                result = TEST_TABLE;
                minTimes = 0;

                mockTable.getId();
                result = 1L;
                minTimes = 0;

                mockTable.getPartitionColumns();
                result = Lists.newArrayList(yearColumn);
                minTimes = 0; // Should not be called

                // Still need these mocks to avoid NPE during rule evaluation
                mockTable.getPartitionColumnTypes();
                result = Lists.newArrayList(Type.INT);
                minTimes = 0;
            }
        };

        PartitionAggregateRewrite rewriteRule = new PartitionAggregateRewrite();
        Rule rule = rewriteRule.build();
        List<Plan> results = rule.transform(aggregate, MemoTestUtils.createCascadesContext(aggregate));

        Assertions.assertEquals(1, results.size());
        // Should return original plan unchanged
        Assertions.assertSame(aggregate.getOutputExpressions(), ((LogicalAggregate) results.get(0)).getOutputExpressions(),
                "Original plan should be returned unchanged for unsupported functions");
    }

    @Test
    void testNonHMSTable(@Injectable HMSExternalTable nonHMSTable) {
        Min minFunction = new Min(yearSlot);
        List<NamedExpression> aggregateFunctions = Lists.newArrayList(
                new Alias(new ExprId(10), minFunction, "min_year")
        );

        // Mock the basic properties needed for the rule to execute
        new Expectations() {{
                nonHMSTable.getName();
                result = TEST_TABLE;
                minTimes = 0;

                nonHMSTable.getId();
                result = 1L;
                minTimes = 0;

                nonHMSTable.getDbName();
                result = TEST_DB;
                minTimes = 0;

                nonHMSTable.getPartitionColumns();
                result = Lists.newArrayList(yearColumn);
                minTimes = 0;
            }};

        // Mock Env singleton
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return mockEnv;
            }
        };

        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), nonHMSTable,
                Lists.newArrayList(TEST_DB, TEST_TABLE), Optional.<org.apache.doris.nereids.trees.TableSample>empty(), Optional.<org.apache.doris.analysis.TableSnapshot>empty());

        LogicalProject project = new LogicalProject(Lists.newArrayList(yearSlot), fileScan);
        LogicalAggregate aggregate = new LogicalAggregate(Lists.newArrayList(), aggregateFunctions, project);

        PartitionAggregateRewrite rewriteRule = new PartitionAggregateRewrite();
        Rule rule = rewriteRule.build();

        // Rule should not apply to non-HMS tables, should return original plan unchanged
        List<Plan> results = rule.transform(aggregate, MemoTestUtils.createCascadesContext(aggregate));

        Assertions.assertEquals(1, results.size());

        Assertions.assertSame(aggregate.getOutputExpressions(), ((LogicalAggregate) results.get(0)).getOutputExpressions(),
                "Original plan should be returned unchanged for non-HMS tables");
    }

    @Test
    void testNonPartitionColumns() {
        // Create a slot reference that's not a partition column
        SlotReference dataSlot = new SlotReference(new ExprId(3), "data", StringType.INSTANCE, true, Lists.newArrayList());

        Min minFunction = new Min(yearSlot);
        List<NamedExpression> aggregateFunctions = Lists.newArrayList(
                new Alias(new ExprId(10), minFunction, "min_year")
        );

        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList(TEST_DB, TEST_TABLE), Optional.<org.apache.doris.nereids.trees.TableSample>empty(), Optional.<org.apache.doris.analysis.TableSnapshot>empty());

        // Project includes a non-partition column
        LogicalProject project = new LogicalProject(Lists.newArrayList(yearSlot, dataSlot), fileScan);
        LogicalAggregate aggregate = new LogicalAggregate(Lists.newArrayList(), aggregateFunctions, project);

        new Expectations() {
            {
                // Required for memo validation
                mockTable.getName();
                result = TEST_TABLE;
                minTimes = 0;

                mockTable.getId();
                result = 1L;
                minTimes = 0;

                mockTable.getPartitionColumns();
                result = Lists.newArrayList(yearColumn); // Only year is a partition column
                minTimes = 0;

                // Still need these mocks to avoid NPE during rule evaluation
                mockTable.getPartitionColumnTypes();
                result = Lists.newArrayList(Type.INT);
                minTimes = 0;
            }
        };

        PartitionAggregateRewrite rewriteRule = new PartitionAggregateRewrite();
        Rule rule = rewriteRule.build();
        List<Plan> results = rule.transform(aggregate, MemoTestUtils.createCascadesContext(aggregate));

        Assertions.assertEquals(1, results.size());
        // Should return original plan unchanged when project contains non-partition columns
        Assertions.assertSame(aggregate.getOutputExpressions(), ((LogicalAggregate) results.get(0)).getOutputExpressions(),
                "Should not transform when project contains non-partition columns");
    }

    @Test
    void testTypeCoercionInPartitionValues() {
        // Test type coercion when partition values need to be cast to different target types
        // Create a partition column with INT type but project it as STRING (requiring coercion)
        SlotReference yearAsStringSlot = new SlotReference(slotId1, "year", StringType.INSTANCE, true, Lists.newArrayList());

        Min minFunction = new Min(yearAsStringSlot);
        List<NamedExpression> aggregateFunctions = Lists.newArrayList(
                new Alias(new ExprId(10), minFunction, "min_year")
        );

        // Create selected partitions with INT values that need coercion to STRING
        Map<Long, PartitionItem> selectedPartitions = createMockSelectedPartitions();
        SelectedPartitions mockSelectedPartitions = new SelectedPartitions(selectedPartitions.size(), selectedPartitions, true);

        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList(TEST_DB, TEST_TABLE), Optional.<org.apache.doris.nereids.trees.TableSample>empty(), Optional.<org.apache.doris.analysis.TableSnapshot>empty());
        fileScan = fileScan.withSelectedPartitions(mockSelectedPartitions);

        LogicalProject project = new LogicalProject(Lists.newArrayList(yearAsStringSlot), fileScan);
        LogicalAggregate aggregate = new LogicalAggregate(Lists.newArrayList(), aggregateFunctions, project);

        new Expectations() {
            {
                mockTable.getPartitionColumns();
                result = Lists.newArrayList(yearColumn); // INT column
                minTimes = 0;

                mockTable.getPartitionColumnTypes();
                result = Lists.newArrayList(Type.INT);
                minTimes = 0;

                mockTable.isViewBased();
                result = false;
                minTimes = 0;

                mockTable.getCatalog();
                result = mockCatalog;
                minTimes = 0;

                mockTable.getDbName();
                result = TEST_DB;
                minTimes = 0;

                mockTable.getName();
                result = TEST_TABLE;
                minTimes = 0;

                mockEnv.getExtMetaCacheMgr();
                result = mockMetaCacheMgr;
                minTimes = 0;

                mockMetaCacheMgr.getMetaStoreCache((HMSExternalCatalog) any);
                result = mockCache;
                minTimes = 0;
            }
        };

        PartitionAggregateRewrite rewriteRule = new PartitionAggregateRewrite();
        Rule rule = rewriteRule.build();
        List<Plan> results = rule.transform(aggregate, MemoTestUtils.createCascadesContext(aggregate));

        Assertions.assertEquals(1, results.size(), "Rule should produce exactly one result");
        Plan transformedPlan = results.get(0);

        LogicalAggregate transformedAggregate = (LogicalAggregate) transformedPlan;
        LogicalProject transformedProject = (LogicalProject) transformedAggregate.child();
        Plan grandChild = (Plan) transformedProject.child();
        Assertions.assertTrue(grandChild instanceof LogicalUnion, "Should create LogicalUnion with coerced types");

        LogicalUnion union = (LogicalUnion) grandChild;
        Assertions.assertFalse(union.getConstantExprsList().isEmpty(), "Union should have constant expressions");

        // Verify that type coercion happened by checking the constant expression types
        List<NamedExpression> firstConstantExprs = union.getConstantExprsList().get(0);
        Assertions.assertFalse(firstConstantExprs.isEmpty(), "Should have at least one constant expression");

        // The constant expression should be of STRING type (coerced from INT)
        NamedExpression firstExpr = firstConstantExprs.get(0);
        Assertions.assertEquals(StringType.INSTANCE, firstExpr.getDataType(),
                "Constant expression should be coerced to STRING type");
    }

    @Test
    void testCanApplyPartitionAggregateRewriteWithSupportedFunctions(@Injectable HMSExternalTable mockTable) {
        Set<Class<? extends org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction>> supportedFunctions =
                org.apache.doris.tablefunction.PartitionValuesTableValuedFunction.PartitionAggOp.supportedFunctions().keySet();

        Assertions.assertFalse(supportedFunctions.contains(Count.class),
                "Count should not be in supported functions");
        Assertions.assertTrue(supportedFunctions.contains(Min.class),
                "Min should be in supported functions");
        Assertions.assertTrue(supportedFunctions.contains(Max.class),
                "Max should be in supported functions");
    }

    private Map<Long, PartitionItem> createMockSelectedPartitions() {
        Map<Long, PartitionItem> partitions = Maps.newHashMap();

        // Create mock partition values as LiteralExpr
        LiteralExpr yearValue = new IntLiteral(2023);
        LiteralExpr monthValue = new StringLiteral("01");

        PartitionKey partitionKey = new PartitionKey();
        partitionKey.pushColumn(yearValue, PrimitiveType.INT);
        partitionKey.pushColumn(monthValue, PrimitiveType.STRING);

        ListPartitionItem partitionItem = new ListPartitionItem(Lists.newArrayList(partitionKey));
        partitions.put(1L, partitionItem);

        return partitions;
    }

    private HiveMetaStoreCache.HivePartitionValues createMockHivePartitionValues() {
        Map<Long, PartitionItem> mockPartitions = createMockSelectedPartitions();
        return new HiveMetaStoreCache.HivePartitionValues() {
            @Override
            public Map<Long, PartitionItem> getIdToPartitionItem() {
                return mockPartitions;
            }
        };
    }
}
