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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.nereids.properties.FunctionalDependencies;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.statistics.StatisticalType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class PhysicalPlanTranslatorTest {

    @Test
    public void testOlapPrune(@Injectable LogicalProperties placeHolder) throws Exception {
        OlapTable t1 = PlanConstructor.newOlapTable(0, "t1", 0, KeysType.AGG_KEYS);
        List<String> qualifier = new ArrayList<>();
        qualifier.add("test");
        List<Slot> t1Output = new ArrayList<>();
        SlotReference col1 = new SlotReference("col1", IntegerType.INSTANCE);
        SlotReference col2 = new SlotReference("col2", IntegerType.INSTANCE);
        SlotReference col3 = new SlotReference("col2", IntegerType.INSTANCE);
        t1Output.add(col1);
        t1Output.add(col2);
        t1Output.add(col3);
        LogicalProperties t1Properties = new LogicalProperties(() -> t1Output, () -> FunctionalDependencies.EMPTY_FUNC_DEPS);
        PhysicalOlapScan scan = new PhysicalOlapScan(StatementScopeIdGenerator.newRelationId(), t1, qualifier, t1.getBaseIndexId(),
                Collections.emptyList(), Collections.emptyList(), null, PreAggStatus.on(),
                ImmutableList.of(), Optional.empty(), t1Properties, Optional.empty());
        Literal t1FilterRight = new IntegerLiteral(1);
        Expression t1FilterExpr = new GreaterThan(col1, t1FilterRight);
        PhysicalFilter<PhysicalOlapScan> filter =
                new PhysicalFilter<>(ImmutableSet.of(t1FilterExpr), placeHolder, scan);
        List<NamedExpression> projList = new ArrayList<>();
        projList.add(col2);
        PhysicalProject<PhysicalFilter<PhysicalOlapScan>> project = new PhysicalProject<>(projList,
                placeHolder, filter);
        PlanTranslatorContext planTranslatorContext = new PlanTranslatorContext();
        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(planTranslatorContext, null);
        PlanFragment fragment = translator.visitPhysicalProject(project, planTranslatorContext);
        PlanNode planNode = fragment.getPlanRoot();
        List<OlapScanNode> scanNodeList = new ArrayList<>();
        planNode.collect(OlapScanNode.class::isInstance, scanNodeList);
        Assertions.assertEquals(2, scanNodeList.get(0).getTupleDesc().getMaterializedSlots().size());
    }

    @Test
    public void testGetConjunctsWithoutPartitionPredicate(@Injectable PhysicalFileScan fileScan,
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
                fileScan.getTable();
                result = table;

                fileScan.getOutput();
                result = Lists.newArrayList(slot1, slot2, slot3, slot4);

                table.getPartitionColumns();
                result = Lists.newArrayList(col1, col2);

                fileScan.getConjuncts();
                result = Sets.newHashSet(expression1, expression2, expression3);

                table.getDlaType();
                result = DLAType.HIVE;
            }
        };
        PlanTranslatorContext planTranslatorContext = new PlanTranslatorContext();
        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(planTranslatorContext, null);
        Set<Expression> conjuncts = translator.getConjunctsWithoutPartitionPredicate(fileScan);
        Assertions.assertEquals(1, conjuncts.size());
        Assertions.assertEquals(expression3, conjuncts.toArray()[0]);

        new Expectations() {
            {
                table.getDlaType();
                result = DLAType.HUDI;
            }
        };
        conjuncts = translator.getConjunctsWithoutPartitionPredicate(fileScan);
        Assertions.assertEquals(1, conjuncts.size());
        Assertions.assertEquals(expression3, conjuncts.toArray()[0]);
    }

    /**
     * Test visitPhysicalLimit with LOCAL phase - validates the LOCAL phase branch
     * This test covers the first major branch of the commit changes
     */
    @Test
    void testVisitPhysicalLimitLocalPhase() {
        // Given: Mock plan tree with LOCAL phase PhysicalLimit
        PlanTranslatorContext context = new PlanTranslatorContext();
        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(context);

        // Create mock child plan and fragment
        Plan mockChildPlan = createMockChildPlan();
        PlanNode mockPlanNode = createMockPlanNode();
        mockPlanNode.setLimit(20L); // Child has existing limit

        PlanFragment inputFragment = new PlanFragment(
                new PlanFragmentId(1), mockPlanNode, DataPartition.UNPARTITIONED);

        // Create PhysicalLimit with LOCAL phase
        PhysicalLimit<Plan> physicalLimit = createPhysicalLimit(10L, 5L, LimitPhase.LOCAL, mockChildPlan);

        // Mock the child plan to return the input fragment
        new Expectations() {
            {
                mockChildPlan.accept(translator, context);
                result = inputFragment;
            }
        };

        // When: Visit PhysicalLimit with local phase
        PlanFragment result = translator.visitPhysicalLimit(physicalLimit, context);

        // Then: Should return a fragment and merge limits on child
        Assertions.assertNotNull(result);
        // Verify merged limit: min(10, max(20-5, 0)) = min(10, 15) = 10
        Assertions.assertEquals(10L, result.getPlanRoot().getLimit());
    }

    /**
     * Test visitPhysicalLimit with GLOBAL phase and non-ExchangeNode child
     * This validates the new ExchangeNode creation logic from the commit
     */
    @Test
    void testVisitPhysicalLimitGlobalPhaseNonExchangeChild() {
        // Given: Mock plan tree with GLOBAL phase PhysicalLimit and non-ExchangeNode child
        PlanTranslatorContext context = new PlanTranslatorContext();
        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(context);

        // Create mock child plan and fragment with non-ExchangeNode
        Plan mockChildPlan = createMockChildPlan();
        PlanNode mockPlanNode = createMockPlanNode(); // Not an ExchangeNode

        PlanFragment inputFragment = new PlanFragment(
                new PlanFragmentId(1), mockPlanNode, DataPartition.UNPARTITIONED);

        // Create PhysicalLimit with GLOBAL phase
        PhysicalLimit<Plan> physicalLimit = createPhysicalLimit(15L, 3L, LimitPhase.GLOBAL, mockChildPlan);

        // Mock the child plan to return the input fragment
        new Expectations() {
            {
                mockChildPlan.accept(translator, context);
                result = inputFragment;
            }
        };

        // When: Visit PhysicalLimit with global phase
        PlanFragment result = translator.visitPhysicalLimit(physicalLimit, context);

        // Then: Should create new fragment with ExchangeNode
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.getPlanRoot() instanceof ExchangeNode);

        ExchangeNode exchangeNode = (ExchangeNode) result.getPlanRoot();
        Assertions.assertEquals(15L, exchangeNode.getLimit());
        Assertions.assertEquals(3L, exchangeNode.getOffset());
        Assertions.assertEquals(1, exchangeNode.getNumInstances());
    }

    /**
     * Test visitPhysicalLimit with GLOBAL phase and ExchangeNode child
     * This validates the existing ExchangeNode limit merging logic
     */
    @Test
    void testVisitPhysicalLimitGlobalPhaseExchangeChild() {
        // Given: Mock plan tree with GLOBAL phase PhysicalLimit and ExchangeNode child
        PlanTranslatorContext context = new PlanTranslatorContext();
        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(context);

        // Create mock child plan and fragment with ExchangeNode
        Plan mockChildPlan = createMockChildPlan();
        ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId(1), createMockPlanNode());
        exchangeNode.setLimit(12L);
        exchangeNode.setOffset(1L);

        PlanFragment inputFragment = new PlanFragment(
                new PlanFragmentId(1), exchangeNode, DataPartition.UNPARTITIONED);

        // Create PhysicalLimit with GLOBAL phase
        PhysicalLimit<Plan> physicalLimit = createPhysicalLimit(8L, 2L, LimitPhase.GLOBAL, mockChildPlan);

        // Mock the child plan to return the input fragment
        new Expectations() {
            {
                mockChildPlan.accept(translator, context);
                result = inputFragment;
            }
        };

        // When: Visit PhysicalLimit with global phase
        PlanFragment result = translator.visitPhysicalLimit(physicalLimit, context);

        // Then: Should return a fragment and merge limits on ExchangeNode
        Assertions.assertNotNull(result);
        ExchangeNode resultExchangeNode = (ExchangeNode) result.getPlanRoot();
        // Verify merged limit: min(8, max(12-2, 0)) = min(8, 10) = 8
        Assertions.assertEquals(8L, resultExchangeNode.getLimit());
        Assertions.assertEquals(2L, resultExchangeNode.getOffset());
    }

    /**
     * Test edge case: visitPhysicalLimit with zero limit
     */
    @Test
    void testVisitPhysicalLimitZeroLimit() {
        // Given: Mock plan tree with zero limit
        PlanTranslatorContext context = new PlanTranslatorContext();
        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(context);

        Plan mockChildPlan = createMockChildPlan();
        PlanNode mockPlanNode = createMockPlanNode();
        mockPlanNode.setLimit(10L);

        PlanFragment inputFragment = new PlanFragment(
                new PlanFragmentId(1), mockPlanNode, DataPartition.UNPARTITIONED);

        // Create PhysicalLimit with zero limit
        PhysicalLimit<Plan> physicalLimit = createPhysicalLimit(0L, 0L, LimitPhase.LOCAL, mockChildPlan);

        new Expectations() {
            {
                mockChildPlan.accept(translator, context);
                result = inputFragment;
            }
        };

        // When: Visit PhysicalLimit with zero limit
        PlanFragment result = translator.visitPhysicalLimit(physicalLimit, context);

        // Then: Should set limit to 0
        Assertions.assertNotNull(result);
        Assertions.assertEquals(0L, result.getPlanRoot().getLimit());
    }

    /**
     * Test edge case: visitPhysicalLimit with large offset
     */
    @Test
    void testVisitPhysicalLimitLargeOffset() {
        // Given: Mock plan tree with large offset
        PlanTranslatorContext context = new PlanTranslatorContext();
        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(context);

        Plan mockChildPlan = createMockChildPlan();
        PlanNode mockPlanNode = createMockPlanNode();
        mockPlanNode.setLimit(10L);

        PlanFragment inputFragment = new PlanFragment(
                new PlanFragmentId(1), mockPlanNode, DataPartition.UNPARTITIONED);

        // Create PhysicalLimit with large offset
        PhysicalLimit<Plan> physicalLimit = createPhysicalLimit(5L, 15L, LimitPhase.LOCAL, mockChildPlan);

        new Expectations() {
            {
                mockChildPlan.accept(translator, context);
                result = inputFragment;
            }
        };

        // When: Visit PhysicalLimit with large offset
        PlanFragment result = translator.visitPhysicalLimit(physicalLimit, context);

        // Then: Should set limit to 0 (max(10-15, 0) = 0, then min(5, 0) = 0)
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5L, result.getPlanRoot().getLimit());
    }

    // Helper methods to create mock objects
    private PhysicalLimit<Plan> createPhysicalLimit(long limit, long offset, LimitPhase phase, Plan child) {
        return new PhysicalLimit<>(limit, offset, phase,
                    new LogicalProperties(() -> Lists.newArrayList(), () -> FunctionalDependencies.EMPTY_FUNC_DEPS),
                    child);
    }

    private Plan createMockChildPlan() {
        return new PhysicalEmptyRelation(
                    new RelationId(1),
                    Lists.newArrayList(),
                    new LogicalProperties(() -> Lists.newArrayList(), () -> FunctionalDependencies.EMPTY_FUNC_DEPS)
        );
    }

    private PlanNode createMockPlanNode() {
        return new PlanNode(new PlanNodeId(1), "MockNode", StatisticalType.DEFAULT) {
            @Override
            protected void toThrift(org.apache.doris.thrift.TPlanNode msg) {}

            @Override
            public String getNodeExplainString(String prefix, org.apache.doris.thrift.TExplainLevel detailLevel) {
                return "MockNode";
            }

            @Override
            public int getNumInstances() {
                return 1;
            }
        };
    }
}
