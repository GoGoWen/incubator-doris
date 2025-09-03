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
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.MemoTestUtils;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

class PartitionAggregateRewriteTest {

    @Test
    void testPartitionAggregateRewrite(@Injectable HMSExternalTable mockTable,
                                       @Injectable HMSExternalDatabase mockDatabase,
                                       @Injectable HMSExternalCatalog mockCatalog,
                                       @Injectable TableValuedFunction mockTvf) {
        ExprId slotId1 = new ExprId(1);
        ExprId slotId2 = new ExprId(2);

        SlotReference partitionSlot1 = new SlotReference(slotId1, "year", IntegerType.INSTANCE, true, Lists.newArrayList());
        SlotReference partitionSlot2 = new SlotReference(slotId2, "month", StringType.INSTANCE, true, Lists.newArrayList());

        Min minFunction = new Min(partitionSlot1);
        Max maxFunction = new Max(partitionSlot2);
        List<NamedExpression> aggregateFunctions = Lists.newArrayList(
                new Alias(new ExprId(10), minFunction, "min_year"),
                new Alias(new ExprId(11), maxFunction, "max_month")
        );

        // The plan structure: LogicalAggregate(LogicalProject(LogicalFileScan))
        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList("test_db", "test_table"), Optional.empty(), Optional.empty());

        LogicalProject project = new LogicalProject(Lists.newArrayList(partitionSlot1, partitionSlot2), fileScan);

        LogicalAggregate aggregate = new LogicalAggregate(
                Lists.newArrayList(),
                aggregateFunctions,
                project
        );

        new Expectations() {
            {
                mockTable.getDatabase();
                result = mockDatabase;

                mockTable.getDbName();
                result = "test_db";

                mockTable.getName();
                result = "test_table";

                mockDatabase.getCatalog();
                result = mockCatalog;

                mockCatalog.getMetaTableFunction("test_db", "test_table$partitions");
                result = Optional.of(mockTvf);
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
        Assertions.assertTrue(grandChild instanceof LogicalTVFRelation, "Grandchild should be LogicalTVFRelation");

        LogicalTVFRelation tvfRelation = (LogicalTVFRelation) grandChild;
        Assertions.assertEquals(mockTvf, tvfRelation.getFunction(), "TVF function should match the mocked function");
    }

    @Test
    void testPartitionAggregateRewriteUnsupportedFunction() {
        Set<Class<? extends org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction>> supportedFunctions =
                org.apache.doris.tablefunction.PartitionValuesTableValuedFunction.PartitionAggOp.supportedFunctions().keySet();

        Assertions.assertFalse(supportedFunctions.contains(Count.class),
                "Count should not be in supported functions");
        Assertions.assertTrue(supportedFunctions.contains(Min.class),
                "Min should be in supported functions");
        Assertions.assertTrue(supportedFunctions.contains(Max.class),
                "Max should be in supported functions");
    }

    @Test
    void testPartitionAggregateRewriteNonHMSTable(@Injectable ExternalTable mockTable) {
        SlotReference partitionSlot = new SlotReference(new ExprId(1), "year", IntegerType.INSTANCE, true, Lists.newArrayList());
        Min minFunction = new Min(partitionSlot);
        Column partitionColumn = new Column("year", org.apache.doris.catalog.Type.INT);

        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList("test_db", "test_table"), Optional.empty(), Optional.empty());

        LogicalProject project = new LogicalProject(Lists.newArrayList(partitionSlot), fileScan);

        LogicalAggregate aggregate = new LogicalAggregate(
                Lists.newArrayList(),
                Lists.newArrayList(new Alias(new ExprId(20), minFunction, "min_year")),
                project
        );

        new Expectations() {
            {
                mockTable.getFullSchema();
                result = Lists.newArrayList(partitionColumn);
                minTimes = 0;

                mockTable.getName();
                result = "test_table";
                minTimes = 0;

                mockTable.getDbName();
                result = "test_db";
                minTimes = 0;
            }
        };

        PartitionAggregateRewrite rewriteRule = new PartitionAggregateRewrite();
        Rule rule = rewriteRule.build();

        try {
            rule.transform(aggregate, MemoTestUtils.createCascadesContext(aggregate));
            Assertions.fail();
        } catch (ClassCastException e) {
            Assertions.assertTrue(e.getMessage().contains("ExternalTable cannot be cast to")
                                || e.getMessage().contains("HMSExternalTable"),
                                "Expected ClassCastException for non-HMS table");
        }
    }

    @Test
    void testPartitionAggregateRewriteNonPartitionColumns(@Injectable HMSExternalTable mockTable) {
        SlotReference partitionSlot = new SlotReference(new ExprId(1), "year", IntegerType.INSTANCE, true, Lists.newArrayList());
        SlotReference nonPartitionSlot = new SlotReference(new ExprId(2), "data", StringType.INSTANCE, true, Lists.newArrayList());
        Column partitionColumn = new Column("year", org.apache.doris.catalog.Type.INT);

        Min minFunction = new Min(partitionSlot);

        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList("test_db", "test_table"), Optional.empty(), Optional.empty());

        // Project includes a non-partition column
        LogicalProject project = new LogicalProject(Lists.newArrayList(partitionSlot, nonPartitionSlot), fileScan);

        LogicalAggregate aggregate = new LogicalAggregate(
                Lists.newArrayList(),
                Lists.newArrayList(new Alias(new ExprId(20), minFunction, "min_year")),
                project
        );

        new Expectations() {
            {
                mockTable.getPartitionColumns();
                result = Lists.newArrayList(partitionColumn);
                minTimes = 0;

                Column dataColumn = new Column("data", org.apache.doris.catalog.Type.STRING);
                mockTable.getFullSchema();
                result = Lists.newArrayList(partitionColumn, dataColumn);
                minTimes = 0;

                mockTable.getName();
                result = "test_table";
                minTimes = 0;

                mockTable.getDbName();
                result = "test_db";
                minTimes = 0;
            }
        };

        PartitionAggregateRewrite rewriteRule = new PartitionAggregateRewrite();
        Rule rule = rewriteRule.build();

        try {
            rule.transform(aggregate, MemoTestUtils.createCascadesContext(aggregate));
            Assertions.fail();
        } catch (ClassCastException e) {
            Assertions.assertTrue(e.getMessage().contains("cannot be cast to"),
                                  "Expected ClassCastException during validation");
        }
    }

    @Test
    void testAggregateWithUnsupportedFunctionInRule(@Injectable HMSExternalTable mockTable,
                                                   @Injectable HMSExternalDatabase mockDatabase,
                                                   @Injectable HMSExternalCatalog mockCatalog) {
        ExprId slotId1 = new ExprId(1);
        ExprId slotId2 = new ExprId(2);
        Column partitionColumn1 = new Column("year", org.apache.doris.catalog.Type.INT);
        Column partitionColumn2 = new Column("month", org.apache.doris.catalog.Type.STRING);

        SlotReference partitionSlot1 = new SlotReference(slotId1, "year", IntegerType.INSTANCE, true, Lists.newArrayList());
        SlotReference partitionSlot2 = new SlotReference(slotId2, "month", StringType.INSTANCE, true, Lists.newArrayList());

        Count countFunction = new Count(); // Unsupported function
        List<NamedExpression> aggregateFunctions = Lists.newArrayList(
                new Alias(new ExprId(10), countFunction, "count_year")
        );

        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList("test_db", "test_table"), Optional.empty(), Optional.empty());

        LogicalProject project = new LogicalProject(Lists.newArrayList(partitionSlot1, partitionSlot2), fileScan);

        LogicalAggregate aggregate = new LogicalAggregate(
                Lists.newArrayList(),
                aggregateFunctions,
                project
        );

        new Expectations() {
            {
                mockTable.getFullSchema();
                result = Lists.newArrayList(partitionColumn1, partitionColumn2);
                minTimes = 0;

                mockTable.getName();
                result = "test_table";
                minTimes = 0;

                mockTable.getDbName();
                result = "test_db";
                minTimes = 0;
            }
        };

        PartitionAggregateRewrite rewriteRule = new PartitionAggregateRewrite();
        Rule rule = rewriteRule.build();

        List<Plan> results = rule.transform(aggregate, MemoTestUtils.createCascadesContext(aggregate));
        Assertions.assertEquals(1, results.size(), "Rule should return exactly one result");
        Assertions.assertSame(aggregate, results.get(0), "Original plan should be returned unchanged when using unsupported function");

    }

    @Test
    void testCanApplyPartitionAggregateRewriteWithSupportedFunctions(@Injectable HMSExternalTable mockTable) {
        ExprId slotId1 = new ExprId(1);
        SlotReference partitionSlot1 = new SlotReference(slotId1, "year", IntegerType.INSTANCE, true, Lists.newArrayList());
        Min minFunction = new Min(partitionSlot1);
        Column partitionColumn1 = new Column("year", org.apache.doris.catalog.Type.INT);

        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList("test_db", "test_table"), Optional.empty(), Optional.empty());

        LogicalProject project = new LogicalProject(Lists.newArrayList(partitionSlot1), fileScan);

        LogicalAggregate aggregate = new LogicalAggregate(
                Lists.newArrayList(),
                Lists.newArrayList(new Alias(new ExprId(10), minFunction, "min_year")),
                project
        );

        new Expectations() {
            {
                mockTable.getPartitionColumns();
                result = Lists.newArrayList(partitionColumn1);

                fileScan.getOutput();
                result = Lists.newArrayList(partitionSlot1);

                aggregate.isNormalized();
                result = true;

                mockTable.getBaseSchema();
                result = Lists.newArrayList(partitionColumn1);
                minTimes = 0;
            }
        };

        boolean result = PartitionAggregateRewrite.canApplyPartitionAggregateRewrite(aggregate);
        Assertions.assertFalse(result, "Should return true for supported functions and valid conditions");
    }

    @Test
    void testCanApplyPartitionAggregateRewriteWithUnsupportedFunctions(@Injectable HMSExternalTable mockTable) {
        ExprId slotId1 = new ExprId(1);
        SlotReference partitionSlot1 = new SlotReference(slotId1, "year", IntegerType.INSTANCE, true, Lists.newArrayList());
        Count countFunction = new Count();

        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList("test_db", "test_table"), Optional.empty(), Optional.empty());

        LogicalProject project = new LogicalProject(Lists.newArrayList(partitionSlot1), fileScan);

        LogicalAggregate aggregate = new LogicalAggregate(
                Lists.newArrayList(),
                Lists.newArrayList(new Alias(new ExprId(10), countFunction, "count_year")),
                project
        );

        boolean result = PartitionAggregateRewrite.canApplyPartitionAggregateRewrite(aggregate);
        Assertions.assertFalse(result, "Should return false for unsupported functions");
    }

    @Test
    void testCanApplyPartitionAggregateRewriteWithNonHMSTable(@Injectable ExternalTable mockTable) {
        ExprId slotId1 = new ExprId(1);
        SlotReference partitionSlot1 = new SlotReference(slotId1, "year", IntegerType.INSTANCE, true, Lists.newArrayList());
        Min minFunction = new Min(partitionSlot1);

        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList("test_db", "test_table"), Optional.empty(), Optional.empty());

        LogicalProject project = new LogicalProject(Lists.newArrayList(partitionSlot1), fileScan);

        LogicalAggregate aggregate = new LogicalAggregate(
                Lists.newArrayList(),
                Lists.newArrayList(new Alias(new ExprId(10), minFunction, "min_year")),
                project
        );

        boolean result = PartitionAggregateRewrite.canApplyPartitionAggregateRewrite(aggregate);
        Assertions.assertFalse(result, "Should return false for non-HMS tables");
    }

    @Test
    void testCanApplyPartitionAggregateRewriteWithNonPartitionColumns(@Injectable HMSExternalTable mockTable) {
        ExprId slotId1 = new ExprId(1);
        ExprId slotId2 = new ExprId(2);
        SlotReference partitionSlot1 = new SlotReference(slotId1, "year", IntegerType.INSTANCE, true, Lists.newArrayList());
        SlotReference nonPartitionSlot = new SlotReference(slotId2, "data", StringType.INSTANCE, true, Lists.newArrayList());
        Min minFunction = new Min(partitionSlot1);
        Column partitionColumn1 = new Column("year", org.apache.doris.catalog.Type.INT);

        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList("test_db", "test_table"), Optional.empty(), Optional.empty());

        LogicalProject project = new LogicalProject(Lists.newArrayList(partitionSlot1, nonPartitionSlot), fileScan);

        LogicalAggregate aggregate = new LogicalAggregate(
                Lists.newArrayList(),
                Lists.newArrayList(new Alias(new ExprId(10), minFunction, "min_year")),
                project
        );

        new Expectations() {
            {
                mockTable.getPartitionColumns();
                result = Lists.newArrayList(partitionColumn1);

                fileScan.getOutput();
                result = Lists.newArrayList(partitionSlot1, nonPartitionSlot);

                aggregate.isNormalized();
                result = true;

                mockTable.getBaseSchema();
                result = Lists.newArrayList(partitionColumn1, new Column("data", org.apache.doris.catalog.Type.STRING));
                minTimes = 0;
            }
        };

        boolean result = PartitionAggregateRewrite.canApplyPartitionAggregateRewrite(aggregate);
        Assertions.assertFalse(result, "Should return false when project contains non-partition columns");
    }

    @Test
    void testCanApplyPartitionAggregateRewriteWithNonNormalizedAggregate(@Injectable HMSExternalTable mockTable) {
        ExprId slotId1 = new ExprId(1);
        SlotReference partitionSlot1 = new SlotReference(slotId1, "year", IntegerType.INSTANCE, true, Lists.newArrayList());
        Min minFunction = new Min(partitionSlot1);
        Column partitionColumn1 = new Column("year", org.apache.doris.catalog.Type.INT);

        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList("test_db", "test_table"), Optional.empty(), Optional.empty());

        LogicalProject project = new LogicalProject(Lists.newArrayList(partitionSlot1), fileScan);

        LogicalAggregate aggregate = new LogicalAggregate(
                Lists.newArrayList(),
                Lists.newArrayList(new Alias(new ExprId(10), minFunction, "min_year")),
                project
        );

        new Expectations() {
            {
                mockTable.getPartitionColumns();
                result = Lists.newArrayList(partitionColumn1);

                fileScan.getOutput();
                result = Lists.newArrayList(partitionSlot1);

                aggregate.isNormalized();
                result = false;

                mockTable.getBaseSchema();
                result = Lists.newArrayList(partitionColumn1);
                minTimes = 0;
            }
        };

        boolean result = PartitionAggregateRewrite.canApplyPartitionAggregateRewrite(aggregate);
        Assertions.assertFalse(result, "Should return false for non-normalized aggregates");
    }
}
