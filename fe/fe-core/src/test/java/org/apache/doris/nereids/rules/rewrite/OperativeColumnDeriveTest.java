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

import org.apache.doris.nereids.rules.rewrite.OperativeColumnDerive.DeriveContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;

public class OperativeColumnDeriveTest {

    private OperativeColumnDerive operativeColumnDerive;
    @Mock private Plan mockPlan;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        operativeColumnDerive = new OperativeColumnDerive();
    }

    @Test
    public void testDeriveContextAddOperativeSlot() {
        DeriveContext context = new DeriveContext();
        NamedExpression slot = new SlotReference("a", IntegerType.INSTANCE);
        context.addOperativeSlot(slot);
        Assertions.assertTrue(context.operativeSlotIds.contains(slot.getExprId().asInt()));

        NamedExpression alias = new Alias(new SlotReference("b", StringType.INSTANCE), "alias_b");
        context.addOperativeSlot(alias);
        Assertions.assertTrue(context.operativeSlotIds.contains(alias.getExprId().asInt()));
    }

    @Test
    public void testDeriveContextAddOperativeSlots() {
        DeriveContext context = new DeriveContext();
        Expression exprTree = new EqualTo(
                new SlotReference("c", IntegerType.INSTANCE),
                new IntegerLiteral(10)
        );
        context.addOperativeSlots(exprTree);
        Assertions.assertTrue(context.operativeSlotIds.contains(((SlotReference) ((EqualTo) exprTree).left()).getExprId().asInt()));
        Assertions.assertFalse(context.operativeSlotIds.contains(10)); // Literal should not be added

        Expression complexExpr = new EqualTo(
                new Alias(new SlotReference("d", StringType.INSTANCE), "alias_d"),
                new SlotReference("e", StringType.INSTANCE)
        );
        context.addOperativeSlots(complexExpr);
        Assertions.assertTrue(context.operativeSlotIds.contains(((SlotReference) ((EqualTo) complexExpr).right()).getExprId().asInt()));
    }

    @Test
    public void testVisitGenericPlan() {
        DeriveContext context = new DeriveContext();

        SlotReference slot1 = new SlotReference("f", IntegerType.INSTANCE);
        SlotReference slot2 = new SlotReference("g", StringType.INSTANCE);

        List<SlotReference> slotReferences = ImmutableList.of(
                slot1,
                slot2
        );
        List<? extends Expression> exprs = slotReferences;

        Mockito.doReturn(exprs).when(mockPlan).getExpressions();
        Mockito.when(mockPlan.children()).thenReturn(ImmutableList.of()); // No children for simplicity

        Plan result = operativeColumnDerive.visit(mockPlan, context);

        Assertions.assertEquals(mockPlan, result);
        Assertions.assertTrue(context.operativeSlotIds.contains(slot1.getExprId().asInt()));
        Assertions.assertTrue(context.operativeSlotIds.contains(slot2.getExprId().asInt()));
    }

    @Test
    public void testVisitLogicalSink() {
        DeriveContext context = new DeriveContext();
        LogicalSink mockSink = Mockito.mock(LogicalSink.class);
        Plan childOfSink = Mockito.mock(Plan.class);

        Mockito.when(mockSink.getExpressions()).thenReturn(ImmutableList.of());
        Mockito.when(mockSink.children()).thenReturn(ImmutableList.of(childOfSink));
        Mockito.when(childOfSink.accept(operativeColumnDerive, context)).thenReturn(childOfSink); // Mock child visit

        Plan result = operativeColumnDerive.visitLogicalSink(mockSink, context);

        Assertions.assertEquals(mockSink, result);
        // Verify that children are visited, but sink itself doesn't add anything specific besides its expressions
    }

    @Test
    public void testVisitLogicalProject() {
        DeriveContext context = new DeriveContext();
        LogicalProject mockProject = Mockito.mock(LogicalProject.class);
        Plan childOfProject = Mockito.mock(Plan.class);

        SlotReference sa = new SlotReference("a", IntegerType.INSTANCE);
        Alias aliasc = new Alias(new SlotReference("c_base", IntegerType.INSTANCE), "c");
        Alias aliasdcomplex = new Alias(new EqualTo(sa, new IntegerLiteral(1)), "d");

        Mockito.when(mockProject.getProjects()).thenReturn(ImmutableList.of(sa, aliasc, aliasdcomplex));
        Mockito.when(mockProject.children()).thenReturn(ImmutableList.of(childOfProject));
        Mockito.when(childOfProject.accept(operativeColumnDerive, context)).thenReturn(childOfProject);

        // Simulate 'c' being operative from a downstream operator
        context.addOperativeSlot(aliasc);

        Plan result = operativeColumnDerive.visitLogicalProject(mockProject, context);

        Assertions.assertEquals(mockProject, result);
        // Check direct slots in projects
        Assertions.assertTrue(context.operativeSlotIds.contains(sa.getExprId().asInt())); // 'a' is a slot, added by generic visit

        // Check alias where child is a Slot (alias_c: c_base -> c)
        // Initially 'c' is operative (12). This should propagate to 'c_base' (12) after visitChildren and back-propagate.
        Assertions.assertTrue(context.operativeSlotIds.contains(aliasc.getExprId().asInt())); // c_base is now operative

        // Check complex alias (alias_d_complex: s_a -> d)
        // Should add s_a's exprId to operative slots
        Assertions.assertTrue(context.operativeSlotIds.contains(sa.getExprId().asInt())); // s_a (ExprId 10) is operative
    }

    @Test
    public void testVisitLogicalUnion() {
        DeriveContext context = new DeriveContext();
        LogicalUnion mockUnion = Mockito.mock(LogicalUnion.class);
        Plan child1 = Mockito.mock(Plan.class);
        Plan child2 = Mockito.mock(Plan.class);

        SlotReference unionOutput1 = new SlotReference("u1", IntegerType.INSTANCE);
        SlotReference unionOutput2 = new SlotReference("u2", StringType.INSTANCE);
        Mockito.when(mockUnion.getOutput()).thenReturn(ImmutableList.of(unionOutput1, unionOutput2));

        SlotReference child1output1 = new SlotReference("c1_1", IntegerType.INSTANCE);
        SlotReference child1output2 = new SlotReference("c1_2", StringType.INSTANCE);
        List<SlotReference> child1Outputs = ImmutableList.of(child1output1, child1output2);

        SlotReference child2output1 = new SlotReference("c2_1", IntegerType.INSTANCE);
        SlotReference child2output2 = new SlotReference("c2_2", StringType.INSTANCE);
        List<SlotReference> child2Outputs = ImmutableList.of(child2output1, child2output2);

        Mockito.when(mockUnion.getRegularChildrenOutputs()).thenReturn(ImmutableList.<List<SlotReference>>of(child1Outputs, child2Outputs));
        Mockito.when(mockUnion.children()).thenReturn(ImmutableList.of(child1, child2));
        Mockito.when(child1.accept(operativeColumnDerive, context)).thenReturn(child1);
        Mockito.when(child2.accept(operativeColumnDerive, context)).thenReturn(child2);

        // Scenario 1: unionOutput1 is operative, should propagate to children
        context.addOperativeSlot(unionOutput1);
        operativeColumnDerive.visitLogicalUnion(mockUnion, context);
        Assertions.assertTrue(context.operativeSlotIds.contains(child1output1.getExprId().asInt())); // child1_output1
        Assertions.assertTrue(context.operativeSlotIds.contains(child2output1.getExprId().asInt())); // child2_output1

        // Clear context for next scenario
        context = new DeriveContext();
        Mockito.when(mockUnion.getOutput()).thenReturn(ImmutableList.of(unionOutput1, unionOutput2)); // Re-mock for new context
        Mockito.when(mockUnion.getRegularChildrenOutputs()).thenReturn(ImmutableList.<List<SlotReference>>of(child1Outputs, child2Outputs));
        Mockito.when(mockUnion.children()).thenReturn(ImmutableList.of(child1, child2));
        Mockito.when(child1.accept(operativeColumnDerive, context)).thenReturn(child1);
        Mockito.when(child2.accept(operativeColumnDerive, context)).thenReturn(child2);

        // Scenario 2: child1_output2 is operative, should back-propagate to unionOutput2
        context.addOperativeSlot(child1output2);
        operativeColumnDerive.visitLogicalUnion(mockUnion, context);
        Assertions.assertTrue(context.operativeSlotIds.contains(unionOutput2.getExprId().asInt())); // unionOutput2
        Assertions.assertTrue(context.operativeSlotIds.contains(child1output2.getExprId().asInt())); // child1_output2
    }

    @Test
    public void testVisitLogicalCatalogRelation() {
        DeriveContext context = new DeriveContext();
        LogicalCatalogRelation mockRelation = Mockito.mock(LogicalCatalogRelation.class);

        SlotReference sx = new SlotReference("x", IntegerType.INSTANCE);
        SlotReference sy = new SlotReference("y", StringType.INSTANCE);
        Mockito.when(mockRelation.getOutput()).thenReturn(ImmutableList.of(sx, sy));
        Mockito.when(mockRelation.withOperativeSlots(Mockito.any())).thenReturn(mockRelation); // Mock the withOperativeSlots call

        // Simulate 'x' being operative
        context.addOperativeSlot(sx);

        Plan result = operativeColumnDerive.visitLogicalCatalogRelation(mockRelation, context);

        Assertions.assertEquals(mockRelation, result);
        // Verify that withOperativeSlots was called with the correct operative slots
        // This is hard to assert directly with Mockito, but we can check if 'x' was considered operative.
        Assertions.assertTrue(context.operativeSlotIds.contains(sx.getExprId().asInt()));
        Assertions.assertFalse(context.operativeSlotIds.contains(sy.getExprId().asInt()));
    }
}
