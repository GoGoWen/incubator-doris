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

package org.apache.doris.nereids.processor.post.materialize;

import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;

public class LazySlotPruningTest {

    private LazySlotPruning lazySlotPruning;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        lazySlotPruning = new LazySlotPruning();
    }

    // Test LazySlotPruning.Context
    @Test
    public void testContextConstructor() throws NoSuchFieldException, IllegalAccessException {
        PhysicalCatalogRelation mockScan = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of()));

        LazySlotPruning.Context context = new LazySlotPruning.Context(mockScan, mockRowIdSlot, mockLazySlots);

        // Use reflection to access private fields
        java.lang.reflect.Field scanField = LazySlotPruning.Context.class.getDeclaredField("scan");
        scanField.setAccessible(true);
        Assertions.assertEquals(mockScan, scanField.get(context));

        java.lang.reflect.Field rowIdSlotField = LazySlotPruning.Context.class.getDeclaredField("rowIdSlot");
        rowIdSlotField.setAccessible(true);
        Assertions.assertEquals(mockRowIdSlot, rowIdSlotField.get(context));

        java.lang.reflect.Field lazySlotsField = LazySlotPruning.Context.class.getDeclaredField("lazySlots");
        lazySlotsField.setAccessible(true);
        Assertions.assertEquals(mockLazySlots, lazySlotsField.get(context));
    }

    @Test
    public void testContextWithLazySlots() throws NoSuchFieldException, IllegalAccessException {
        PhysicalCatalogRelation mockScan = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        List<Slot> initialLazySlots = ImmutableList.of(new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of()));
        List<Slot> newLazySlots = ImmutableList.of(new SlotReference(new ExprId(3), "lazy2", IntegerType.INSTANCE, true, ImmutableList.of()));

        LazySlotPruning.Context initialContext = new LazySlotPruning.Context(mockScan, mockRowIdSlot, initialLazySlots);
        LazySlotPruning.Context newContext = initialContext.withLazySlots(newLazySlots);

        Assertions.assertNotEquals(initialContext, newContext);

        java.lang.reflect.Field scanField = LazySlotPruning.Context.class.getDeclaredField("scan");
        scanField.setAccessible(true);
        Assertions.assertEquals(mockScan, scanField.get(newContext));

        java.lang.reflect.Field rowIdSlotField = LazySlotPruning.Context.class.getDeclaredField("rowIdSlot");
        rowIdSlotField.setAccessible(true);
        Assertions.assertEquals(mockRowIdSlot, rowIdSlotField.get(newContext));

        java.lang.reflect.Field lazySlotsField = LazySlotPruning.Context.class.getDeclaredField("lazySlots");
        lazySlotsField.setAccessible(true);
        Assertions.assertEquals(newLazySlots, lazySlotsField.get(newContext));
    }

    @Test
    public void testContextForceRowIdNullable() throws NoSuchFieldException, IllegalAccessException {
        PhysicalCatalogRelation mockScan = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference initialRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, false, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of()));

        LazySlotPruning.Context context = new LazySlotPruning.Context(mockScan, initialRowIdSlot, mockLazySlots);
        context.forceRowIdNullable();

        java.lang.reflect.Field rowIdSlotField = LazySlotPruning.Context.class.getDeclaredField("rowIdSlot");
        rowIdSlotField.setAccessible(true);
        SlotReference currentRowIdSlot = (SlotReference) rowIdSlotField.get(context);
        Assertions.assertTrue(currentRowIdSlot.nullable());
    }

    @Test
    public void testContextUpdateRowIdSlot() throws NoSuchFieldException, IllegalAccessException {
        PhysicalCatalogRelation mockScan = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference initialRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        SlotReference updatedRowIdSlot = new SlotReference(new ExprId(4), "new_rowid", StringType.INSTANCE, true, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of()));

        LazySlotPruning.Context context = new LazySlotPruning.Context(mockScan, initialRowIdSlot, mockLazySlots);
        context.updateRowIdSlot(updatedRowIdSlot);

        java.lang.reflect.Field rowIdSlotField = LazySlotPruning.Context.class.getDeclaredField("rowIdSlot");
        rowIdSlotField.setAccessible(true);
        Assertions.assertEquals(updatedRowIdSlot, rowIdSlotField.get(context));
    }

    // Test visit(Plan, Context)
    @Test
    public void testVisitGenericPlanNoChildContainingLazySlots() {
        PhysicalCatalogRelation mockScan = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of()));
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockScan, mockRowIdSlot, mockLazySlots);

        Plan mockChild1 = Mockito.mock(Plan.class);
        Mockito.when(mockChild1.getOutput()).thenReturn(ImmutableList.of(new SlotReference(new ExprId(3), "other1", IntegerType.INSTANCE, true, ImmutableList.of())));
        Mockito.when(mockChild1.children()).thenReturn(ImmutableList.of()); // Stop recursion

        Plan mockParent = Mockito.mock(AbstractPhysicalPlan.class); // Use AbstractPhysicalPlan for withChildren etc.
        Mockito.when(mockParent.arity()).thenReturn(1);
        Mockito.when(mockParent.children()).thenReturn(ImmutableList.of(mockChild1));
        Mockito.when(mockParent.withChildren(Mockito.<Plan>any())).thenReturn(mockParent); // Ensure withChildren returns same mock for simplicity

        Plan result = lazySlotPruning.visit(mockParent, context);

        Assertions.assertEquals(mockParent, result);
        Mockito.verify(mockChild1, Mockito.never()).accept(Mockito.any(), Mockito.any()); // Child should not be visited if it doesn't contain lazy slots
    }

    @Test
    public void testVisitPhysicalOlapScan() {
        PhysicalCatalogRelation mockScan = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of()));
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockScan, mockRowIdSlot, mockLazySlots);

        PhysicalOlapScan olapScan = Mockito.mock(PhysicalOlapScan.class);
        Plan result = lazySlotPruning.visitPhysicalOlapScan(olapScan, context);
        Assertions.assertEquals(olapScan, result);
    }

    @Test
    public void testVisitPhysicalFileScanHappyPath() throws NoSuchFieldException, IllegalAccessException {
        PhysicalFileScan mockFileScan = Mockito.mock(PhysicalFileScan.class);
        ExternalTable mockTable = Mockito.mock(ExternalTable.class);
        Mockito.when(mockFileScan.getTable()).thenReturn(mockTable);
        Mockito.when(mockFileScan.getOutput()).thenReturn(ImmutableList.of(
                new SlotReference(new ExprId(10), "f1", IntegerType.INSTANCE, true, ImmutableList.of()),
                new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of())));

        PhysicalCatalogRelation mockCatalogRelation = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of()));
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockCatalogRelation, mockRowIdSlot, mockLazySlots);

        Plan result = lazySlotPruning.visitPhysicalFileScan(mockFileScan, context);

        Assertions.assertTrue(result instanceof PhysicalLazyMaterializeFileScan);
        PhysicalLazyMaterializeFileScan lazyMaterializeFileScan = (PhysicalLazyMaterializeFileScan) result;

        java.lang.reflect.Field scanField = PhysicalLazyMaterializeFileScan.class.getDeclaredField("scan");
        scanField.setAccessible(true);
        Assertions.assertEquals(mockFileScan, scanField.get(lazyMaterializeFileScan));

        java.lang.reflect.Field rowIdField = PhysicalLazyMaterializeFileScan.class.getDeclaredField("rowId");
        rowIdField.setAccessible(true);
        Assertions.assertEquals(mockRowIdSlot, rowIdField.get(lazyMaterializeFileScan));

        java.lang.reflect.Field lazySlotsField = PhysicalLazyMaterializeFileScan.class.getDeclaredField("lazySlots");
        lazySlotsField.setAccessible(true);
        Assertions.assertEquals(mockLazySlots, lazySlotsField.get(lazyMaterializeFileScan));
    }

    @Test
    public void testVisitPhysicalFileScanLazyMaterializeFault() {
        PhysicalFileScan mockFileScan = Mockito.mock(PhysicalFileScan.class);
        Mockito.when(mockFileScan.getOutput()).thenReturn(ImmutableList.of(
                new SlotReference(new ExprId(10), "f1", IntegerType.INSTANCE, true, ImmutableList.of()))); // Does not contain lazy slot

        PhysicalCatalogRelation mockCatalogRelation = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of()));
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockCatalogRelation, mockRowIdSlot, mockLazySlots);

        Assertions.assertThrows(RuntimeException.class, () -> {
            lazySlotPruning.visitPhysicalFileScan(mockFileScan, context);
        }, "Lazy materialize fault");
    }

    @Test
    public void testVisitPhysicalProjectNoAlias() {
        PhysicalProject mockProject = Mockito.mock(PhysicalProject.class);
        Plan childOfProject = Mockito.mock(Plan.class);
        Mockito.when(mockProject.getProjects()).thenReturn(ImmutableList.of(
                new SlotReference(new ExprId(100), "p1", IntegerType.INSTANCE, true, ImmutableList.of())));
        Mockito.when(mockProject.child()).thenReturn(childOfProject);

        PhysicalCatalogRelation mockCatalogRelation = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of()));
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockCatalogRelation, mockRowIdSlot, mockLazySlots);

        Mockito.when(childOfProject.accept(Mockito.any(LazySlotPruning.class), Mockito.any(LazySlotPruning.Context.class))).thenReturn(childOfProject);

        Plan result = lazySlotPruning.visitPhysicalProject(mockProject, context);
        Assertions.assertEquals(mockProject, result);
        Mockito.verify(childOfProject, Mockito.times(1)).accept(lazySlotPruning, context);
    }

    @Test
    public void testVisitAbstractPhysicalJoinNoLazySlotsInChild() {
        AbstractPhysicalJoin mockJoin = Mockito.mock(AbstractPhysicalJoin.class);
        Plan mockLeftChild = Mockito.mock(Plan.class);
        Plan mockRightChild = Mockito.mock(Plan.class);

        Mockito.when(mockLeftChild.getOutputSet()).thenReturn(ImmutableSet.of(new SlotReference(new ExprId(100), "left_a", IntegerType.INSTANCE, true, ImmutableList.of())));
        Mockito.when(mockRightChild.getOutputSet()).thenReturn(ImmutableSet.of(new SlotReference(new ExprId(101), "right_b", IntegerType.INSTANCE, true, ImmutableList.of())));
        Mockito.when(mockJoin.arity()).thenReturn(2);
        Mockito.when(mockJoin.child(0)).thenReturn(mockLeftChild);
        Mockito.when(mockJoin.child(1)).thenReturn(mockRightChild);
        Mockito.when(mockLeftChild.accept(Mockito.any(LazySlotPruning.class), Mockito.any(LazySlotPruning.Context.class))).thenReturn(mockLeftChild);
        Mockito.when(mockRightChild.accept(Mockito.any(LazySlotPruning.class), Mockito.any(LazySlotPruning.Context.class))).thenReturn(mockRightChild);

        PhysicalCatalogRelation mockCatalogRelation = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of()));
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockCatalogRelation, mockRowIdSlot, mockLazySlots);

        Plan result = lazySlotPruning.visitAbstractPhysicalJoin(mockJoin, context);

        Assertions.assertEquals(mockJoin, result);
        Mockito.verify(mockLeftChild, Mockito.never()).accept(Mockito.any(), Mockito.any()); // No lazy slots in child, so not visited
        Mockito.verify(mockRightChild, Mockito.never()).accept(Mockito.any(), Mockito.any()); // No lazy slots in child, so not visited
    }

    @Test
    public void testVisitAbstractPhysicalJoinWithLazySlotsInChildAndNoChange() {
        AbstractPhysicalJoin mockJoin = Mockito.mock(AbstractPhysicalJoin.class);
        AbstractPhysicalPlan mockLeftChild = Mockito.mock(AbstractPhysicalPlan.class);
        Plan mockRightChild = Mockito.mock(Plan.class);

        SlotReference lazySlot = new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of());
        Mockito.when(mockLeftChild.getOutputSet()).thenReturn(ImmutableSet.of(lazySlot));
        Mockito.when(mockLeftChild.arity()).thenReturn(0);
        Mockito.when(mockLeftChild.children()).thenReturn(ImmutableList.of());
        Mockito.when(mockRightChild.getOutputSet()).thenReturn(ImmutableSet.of(new SlotReference(new ExprId(101), "right_b", IntegerType.INSTANCE, true, ImmutableList.of())));
        Mockito.when(mockRightChild.arity()).thenReturn(0);
        Mockito.when(mockRightChild.children()).thenReturn(ImmutableList.of());

        Mockito.when(mockJoin.arity()).thenReturn(2);
        Mockito.when(mockJoin.child(0)).thenReturn(mockLeftChild);
        Mockito.when(mockJoin.child(1)).thenReturn(mockRightChild);

        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, false, ImmutableList.of());

        // Mock withChildren to return a new mock if children change
        AbstractPhysicalPlan modifiedJoin = Mockito.mock(AbstractPhysicalPlan.class);
        Mockito.when(mockJoin.withChildren(Mockito.<Plan>any())).thenReturn(modifiedJoin);
        Mockito.when(modifiedJoin.copyStatsAndGroupIdFrom(Mockito.any())).thenReturn(modifiedJoin);
        Mockito.when(modifiedJoin.resetLogicalProperties()).thenReturn(modifiedJoin);
        Mockito.when(modifiedJoin.getOutput()).thenReturn(ImmutableList.of(lazySlot, mockRowIdSlot)); // Simplified output
        Mockito.when(modifiedJoin.child(0)).thenReturn(mockLeftChild);
        Mockito.when(modifiedJoin.child(1)).thenReturn(mockRightChild);

        PhysicalCatalogRelation mockCatalogRelation = Mockito.mock(PhysicalCatalogRelation.class);
        List<Slot> mockLazySlots = ImmutableList.of(lazySlot);
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockCatalogRelation, mockRowIdSlot, mockLazySlots);

        Mockito.when(mockLeftChild.accept(Mockito.any(LazySlotPruning.class), Mockito.any(LazySlotPruning.Context.class))).thenReturn(mockLeftChild); // Child not actually changed

        Plan result = lazySlotPruning.visitAbstractPhysicalJoin(mockJoin, context);

        Assertions.assertEquals(mockJoin, result); // No change in join itself, only child processed
        Mockito.verify(mockLeftChild, Mockito.times(1)).accept(Mockito.any(), Mockito.any());
        Mockito.verify(mockRightChild, Mockito.times(0)).accept(Mockito.any(), Mockito.any());
        Mockito.verify(mockJoin, Mockito.never()).withChildren(Mockito.<List<Plan>>any()); // Should not call withChildren if child didn't change
    }

    // Test other simple visit methods
    @Test
    public void testVisitPhysicalHashAggregate() {
        PhysicalCatalogRelation mockScan = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of()));
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockScan, mockRowIdSlot, mockLazySlots);

        PhysicalHashAggregate aggregate = Mockito.mock(PhysicalHashAggregate.class);
        Plan result = lazySlotPruning.visitPhysicalHashAggregate(aggregate, context);
        Assertions.assertEquals(aggregate, result);
    }

    @Test
    public void testVisitPhysicalCTEConsumer() {
        PhysicalCatalogRelation mockScan = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of()));
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockScan, mockRowIdSlot, mockLazySlots);

        PhysicalCTEConsumer cteConsumer = Mockito.mock(PhysicalCTEConsumer.class);
        Plan result = lazySlotPruning.visitPhysicalCTEConsumer(cteConsumer, context);
        Assertions.assertEquals(cteConsumer, result);
    }

    @Test
    public void testVisitPhysicalCTEProducer() {
        PhysicalCatalogRelation mockScan = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of()));
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockScan, mockRowIdSlot, mockLazySlots);

        PhysicalCTEProducer producer = Mockito.mock(PhysicalCTEProducer.class);
        Plan result = lazySlotPruning.visitPhysicalCTEProducer(producer, context);
        Assertions.assertEquals(producer, result);
    }

    @Test
    public void testVisitPhysicalRepeat() {
        PhysicalCatalogRelation mockScan = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of()));
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockScan, mockRowIdSlot, mockLazySlots);

        PhysicalRepeat repeat = Mockito.mock(PhysicalRepeat.class);
        Plan result = lazySlotPruning.visitPhysicalRepeat(repeat, context);
        Assertions.assertEquals(repeat, result);
    }

    @Test
    public void testVisitPhysicalSetOperation() {
        PhysicalCatalogRelation mockScan = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of()));
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockScan, mockRowIdSlot, mockLazySlots);

        PhysicalSetOperation setOperation = Mockito.mock(PhysicalSetOperation.class);
        Plan result = lazySlotPruning.visitPhysicalSetOperation(setOperation, context);
        Assertions.assertEquals(setOperation, result);
    }

    @Test
    public void testVisitPhysicalOneRowRelation() {
        PhysicalCatalogRelation mockScan = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of()));
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockScan, mockRowIdSlot, mockLazySlots);

        PhysicalOneRowRelation oneRowRelation = Mockito.mock(PhysicalOneRowRelation.class);
        Plan result = lazySlotPruning.visitPhysicalOneRowRelation(oneRowRelation, context);
        Assertions.assertEquals(oneRowRelation, result);
    }

    // Test visit(Plan, Context) with children containing lazy slots
    @Test
    public void testVisitGenericPlanWithChildContainingLazySlots() {
        PhysicalCatalogRelation mockScan = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        SlotReference lazySlot = new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(lazySlot);
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockScan, mockRowIdSlot, mockLazySlots);

        Plan mockChild1 = Mockito.mock(Plan.class);
        Mockito.when(mockChild1.getOutput()).thenReturn(ImmutableList.of(lazySlot));
        Mockito.when(mockChild1.children()).thenReturn(ImmutableList.of());
        Mockito.when(mockChild1.accept(Mockito.any(LazySlotPruning.class), Mockito.eq(context))).thenReturn(mockChild1);

        Plan mockChild2 = Mockito.mock(Plan.class);
        Mockito.when(mockChild2.getOutput()).thenReturn(ImmutableList.of(new SlotReference(new ExprId(3), "other1", IntegerType.INSTANCE, true, ImmutableList.of())));
        Mockito.when(mockChild2.children()).thenReturn(ImmutableList.of());

        AbstractPhysicalPlan mockParent = Mockito.mock(AbstractPhysicalPlan.class);
        Mockito.when(mockParent.arity()).thenReturn(2);
        Mockito.when(mockParent.children()).thenReturn(ImmutableList.of(mockChild1, mockChild2));
        Mockito.when(mockParent.getOutput()).thenReturn(ImmutableList.of(mockRowIdSlot));
        AbstractPhysicalPlan modifiedParent = Mockito.mock(AbstractPhysicalPlan.class);
        Mockito.when(mockParent.withChildren(Mockito.<List<Plan>>any())).thenReturn(modifiedParent);
        Mockito.when(modifiedParent.copyStatsAndGroupIdFrom(mockParent)).thenReturn(modifiedParent);
        Mockito.when(modifiedParent.resetLogicalProperties()).thenReturn(modifiedParent);

        lazySlotPruning.visit(mockParent, context);

        Mockito.verify(mockChild1, Mockito.times(1)).accept(lazySlotPruning, context);
        Mockito.verify(mockChild2, Mockito.never()).accept(Mockito.any(), Mockito.any());
    }

    @Test
    public void testVisitGenericPlanWithChildChanged() {
        PhysicalCatalogRelation mockScan = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        SlotReference lazySlot = new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(lazySlot);
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockScan, mockRowIdSlot, mockLazySlots);

        Plan mockChild1 = Mockito.mock(Plan.class);
        Mockito.when(mockChild1.getOutput()).thenReturn(ImmutableList.of(lazySlot));
        Mockito.when(mockChild1.children()).thenReturn(ImmutableList.of());
        Plan modifiedChild1 = Mockito.mock(Plan.class);
        Mockito.when(mockChild1.accept(Mockito.any(LazySlotPruning.class), Mockito.eq(context))).thenReturn(modifiedChild1);

        AbstractPhysicalPlan mockParent = Mockito.mock(AbstractPhysicalPlan.class);
        Mockito.when(mockParent.arity()).thenReturn(1);
        Mockito.when(mockParent.children()).thenReturn(ImmutableList.of(mockChild1));
        Mockito.when(mockParent.getOutput()).thenReturn(ImmutableList.of(mockRowIdSlot));
        AbstractPhysicalPlan modifiedParent = Mockito.mock(AbstractPhysicalPlan.class);
        Mockito.when(mockParent.withChildren(Mockito.<List<Plan>>any())).thenReturn(modifiedParent);
        Mockito.when(modifiedParent.copyStatsAndGroupIdFrom(mockParent)).thenReturn(modifiedParent);
        Mockito.when(modifiedParent.resetLogicalProperties()).thenReturn(modifiedParent);

        lazySlotPruning.visit(mockParent, context);

        Mockito.verify(mockParent, Mockito.times(1)).withChildren(Mockito.<List<Plan>>any());
    }

    // Test visitPhysicalProject with alias
    @Test
    public void testVisitPhysicalProjectWithAlias() {
        PhysicalCatalogRelation mockCatalogRelation = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        SlotReference childSlot = new SlotReference(new ExprId(10), "child_col", IntegerType.INSTANCE, true, ImmutableList.of());
        // Create alias and use its toSlot() result as the lazy slot
        // Use the same ExprId for alias so toSlot() returns a slot with matching ExprId
        ExprId aliasExprId = new ExprId(2);
        Alias alias = new Alias(aliasExprId, childSlot, "lazy1");
        Slot aliasSlot = alias.toSlot();
        List<Slot> mockLazySlots = ImmutableList.of(aliasSlot);
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockCatalogRelation, mockRowIdSlot, mockLazySlots);

        PhysicalProject mockProject = Mockito.mock(PhysicalProject.class);
        Plan childOfProject = Mockito.mock(Plan.class);
        Mockito.when(mockProject.getProjects()).thenReturn(ImmutableList.of((NamedExpression) alias));
        Mockito.when(mockProject.child()).thenReturn(childOfProject);
        Mockito.when(childOfProject.getOutput()).thenReturn(ImmutableList.of(mockRowIdSlot));
        // Mock getOutput() for the project to prevent NPE in resetLogicalProperties()
        Mockito.when(mockProject.getOutput()).thenReturn(ImmutableList.of(aliasSlot));

        Mockito.when(childOfProject.accept(Mockito.any(LazySlotPruning.class), Mockito.any(LazySlotPruning.Context.class))).thenReturn(childOfProject);

        // Mock withProjectionsAndChild to return a new project
        PhysicalProject newProject = Mockito.mock(PhysicalProject.class);
        Mockito.when(newProject.getOutput()).thenReturn(ImmutableList.of(mockRowIdSlot));
        Mockito.when(mockProject.withProjectionsAndChild(Mockito.anyList(), Mockito.any(Plan.class))).thenReturn(newProject);
        Mockito.when(newProject.resetLogicalProperties()).thenReturn(newProject);

        Plan result = lazySlotPruning.visitPhysicalProject(mockProject, context);
        Assertions.assertNotNull(result);
        Mockito.verify(childOfProject, Mockito.times(1)).accept(Mockito.any(LazySlotPruning.class), Mockito.any(LazySlotPruning.Context.class));
    }

    @Test
    public void testVisitPhysicalProjectWithRowIdSlot() {
        PhysicalCatalogRelation mockCatalogRelation = Mockito.mock(PhysicalCatalogRelation.class);
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, true, ImmutableList.of());
        SlotReference lazySlot = new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference otherSlot = new SlotReference(new ExprId(3), "other1", IntegerType.INSTANCE, true, ImmutableList.of());
        List<Slot> mockLazySlots = ImmutableList.of(lazySlot);
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockCatalogRelation, mockRowIdSlot, mockLazySlots);

        PhysicalProject mockProject = Mockito.mock(PhysicalProject.class);
        Plan childOfProject = Mockito.mock(Plan.class);
        Mockito.when(mockProject.getProjects()).thenReturn(ImmutableList.of(
                (NamedExpression) lazySlot,
                (NamedExpression) otherSlot));
        Mockito.when(mockProject.child()).thenReturn(childOfProject);
        Mockito.when(childOfProject.getOutput()).thenReturn(ImmutableList.of(mockRowIdSlot));
        Mockito.when(childOfProject.accept(Mockito.any(LazySlotPruning.class), Mockito.any(LazySlotPruning.Context.class))).thenReturn(childOfProject);

        PhysicalProject modifiedProject = Mockito.mock(PhysicalProject.class);
        Mockito.when(mockProject.withProjectionsAndChild(Mockito.any(), Mockito.any())).thenReturn(modifiedProject);
        Mockito.when(modifiedProject.resetLogicalProperties()).thenReturn(modifiedProject);

        Plan result = lazySlotPruning.visitPhysicalProject(mockProject, context);
        Assertions.assertNotNull(result);
        Mockito.verify(mockProject, Mockito.times(1)).withProjectionsAndChild(Mockito.any(), Mockito.eq(childOfProject));
    }

    // Test visitAbstractPhysicalJoin with different join types and nullable updates
    @Test
    public void testVisitAbstractPhysicalJoinWithFullOuterJoin() throws NoSuchFieldException, IllegalAccessException {
        AbstractPhysicalJoin mockJoin = Mockito.mock(AbstractPhysicalJoin.class);
        AbstractPhysicalPlan mockLeftChild = Mockito.mock(AbstractPhysicalPlan.class);
        AbstractPhysicalPlan mockRightChild = Mockito.mock(AbstractPhysicalPlan.class);

        SlotReference lazySlot = new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, false, ImmutableList.of());

        Mockito.when(mockLeftChild.getOutputSet()).thenReturn(ImmutableSet.of(lazySlot));
        Mockito.when(mockRightChild.getOutputSet()).thenReturn(ImmutableSet.of());
        Mockito.when(mockJoin.arity()).thenReturn(2);
        Mockito.when(mockJoin.child(0)).thenReturn(mockLeftChild);
        Mockito.when(mockJoin.child(1)).thenReturn(mockRightChild);
        Mockito.when(mockJoin.getJoinType()).thenReturn(JoinType.FULL_OUTER_JOIN);

        AbstractPhysicalPlan modifiedJoin = Mockito.mock(AbstractPhysicalPlan.class);
        Mockito.when(mockJoin.withChildren(Mockito.<List<Plan>>any())).thenReturn(modifiedJoin);
        Mockito.when(modifiedJoin.copyStatsAndGroupIdFrom(mockJoin)).thenReturn(modifiedJoin);
        Mockito.when(modifiedJoin.resetLogicalProperties()).thenReturn(modifiedJoin);
        // Use mockRowIdSlot in output so indexOf can find it (equals compares ExprId)
        Mockito.when(modifiedJoin.getOutput()).thenReturn(ImmutableList.of(lazySlot, mockRowIdSlot));
        Mockito.when(modifiedJoin.child(0)).thenReturn(mockLeftChild);
        Mockito.when(modifiedJoin.child(1)).thenReturn(mockRightChild);

        PhysicalCatalogRelation mockCatalogRelation = Mockito.mock(PhysicalCatalogRelation.class);
        List<Slot> mockLazySlots = ImmutableList.of(lazySlot);
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockCatalogRelation, mockRowIdSlot, mockLazySlots);

        // Return a different child to trigger hasNewChildren = true
        Plan modifiedLeftChild = Mockito.mock(Plan.class);
        Mockito.when(mockLeftChild.accept(Mockito.any(LazySlotPruning.class), Mockito.any(LazySlotPruning.Context.class))).thenReturn(modifiedLeftChild);
        Mockito.when(modifiedJoin.child(0)).thenReturn(modifiedLeftChild);

        lazySlotPruning.visitAbstractPhysicalJoin(mockJoin, context);

        java.lang.reflect.Field rowIdSlotField = LazySlotPruning.Context.class.getDeclaredField("rowIdSlot");
        rowIdSlotField.setAccessible(true);
        SlotReference updatedRowIdSlot = (SlotReference) rowIdSlotField.get(context);
        Assertions.assertTrue(updatedRowIdSlot.nullable());
    }

    @Test
    public void testVisitAbstractPhysicalJoinWithLeftOuterJoin() throws NoSuchFieldException, IllegalAccessException {
        AbstractPhysicalJoin mockJoin = Mockito.mock(AbstractPhysicalJoin.class);
        AbstractPhysicalPlan mockLeftChild = Mockito.mock(AbstractPhysicalPlan.class);
        AbstractPhysicalPlan mockRightChild = Mockito.mock(AbstractPhysicalPlan.class);

        SlotReference lazySlot = new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, false, ImmutableList.of());

        Mockito.when(mockLeftChild.getOutputSet()).thenReturn(ImmutableSet.of(lazySlot));
        Mockito.when(mockRightChild.getOutputSet()).thenReturn(ImmutableSet.of());
        // For LEFT_OUTER_JOIN, rowIdSlot should be in right child's output
        Mockito.when(mockRightChild.getOutput()).thenReturn(ImmutableList.of(mockRowIdSlot));
        Mockito.when(mockJoin.arity()).thenReturn(2);
        Mockito.when(mockJoin.child(0)).thenReturn(mockLeftChild);
        Mockito.when(mockJoin.child(1)).thenReturn(mockRightChild);
        Mockito.when(mockJoin.getJoinType()).thenReturn(JoinType.LEFT_OUTER_JOIN);

        AbstractPhysicalPlan modifiedJoin = Mockito.mock(AbstractPhysicalPlan.class);
        Mockito.when(mockJoin.withChildren(Mockito.<List<Plan>>any())).thenReturn(modifiedJoin);
        Mockito.when(modifiedJoin.copyStatsAndGroupIdFrom(mockJoin)).thenReturn(modifiedJoin);
        Mockito.when(modifiedJoin.resetLogicalProperties()).thenReturn(modifiedJoin);
        // Use mockRowIdSlot in output so indexOf can find it (equals compares ExprId)
        Mockito.when(modifiedJoin.getOutput()).thenReturn(ImmutableList.of(lazySlot, mockRowIdSlot));
        Mockito.when(modifiedJoin.child(0)).thenReturn(mockLeftChild);
        Mockito.when(modifiedJoin.child(1)).thenReturn(mockRightChild);

        PhysicalCatalogRelation mockCatalogRelation = Mockito.mock(PhysicalCatalogRelation.class);
        List<Slot> mockLazySlots = ImmutableList.of(lazySlot);
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockCatalogRelation, mockRowIdSlot, mockLazySlots);

        // Return a different child to trigger hasNewChildren = true
        Plan modifiedLeftChild = Mockito.mock(Plan.class);
        Mockito.when(mockLeftChild.accept(Mockito.any(LazySlotPruning.class), Mockito.any(LazySlotPruning.Context.class))).thenReturn(modifiedLeftChild);
        Mockito.when(modifiedJoin.child(0)).thenReturn(modifiedLeftChild);

        lazySlotPruning.visitAbstractPhysicalJoin(mockJoin, context);

        java.lang.reflect.Field rowIdSlotField = LazySlotPruning.Context.class.getDeclaredField("rowIdSlot");
        rowIdSlotField.setAccessible(true);
        SlotReference updatedRowIdSlot = (SlotReference) rowIdSlotField.get(context);
        Assertions.assertTrue(updatedRowIdSlot.nullable());
    }

    @Test
    public void testVisitAbstractPhysicalJoinWithRightOuterJoin() throws NoSuchFieldException, IllegalAccessException {
        AbstractPhysicalJoin mockJoin = Mockito.mock(AbstractPhysicalJoin.class);
        AbstractPhysicalPlan mockLeftChild = Mockito.mock(AbstractPhysicalPlan.class);
        AbstractPhysicalPlan mockRightChild = Mockito.mock(AbstractPhysicalPlan.class);

        SlotReference lazySlot = new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, false, ImmutableList.of());

        Mockito.when(mockLeftChild.getOutputSet()).thenReturn(ImmutableSet.of());
        // For RIGHT_OUTER_JOIN, rowIdSlot should be in left child's output
        Mockito.when(mockLeftChild.getOutput()).thenReturn(ImmutableList.of(mockRowIdSlot));
        Mockito.when(mockRightChild.getOutputSet()).thenReturn(ImmutableSet.of(lazySlot));
        Mockito.when(mockJoin.arity()).thenReturn(2);
        Mockito.when(mockJoin.child(0)).thenReturn(mockLeftChild);
        Mockito.when(mockJoin.child(1)).thenReturn(mockRightChild);
        Mockito.when(mockJoin.getJoinType()).thenReturn(JoinType.RIGHT_OUTER_JOIN);

        AbstractPhysicalPlan modifiedJoin = Mockito.mock(AbstractPhysicalPlan.class);
        Mockito.when(mockJoin.withChildren(Mockito.<List<Plan>>any())).thenReturn(modifiedJoin);
        Mockito.when(modifiedJoin.copyStatsAndGroupIdFrom(mockJoin)).thenReturn(modifiedJoin);
        Mockito.when(modifiedJoin.resetLogicalProperties()).thenReturn(modifiedJoin);
        // Use mockRowIdSlot in output so indexOf can find it (equals compares ExprId)
        Mockito.when(modifiedJoin.getOutput()).thenReturn(ImmutableList.of(lazySlot, mockRowIdSlot));
        Mockito.when(modifiedJoin.child(0)).thenReturn(mockLeftChild);
        Mockito.when(modifiedJoin.child(1)).thenReturn(mockRightChild);

        PhysicalCatalogRelation mockCatalogRelation = Mockito.mock(PhysicalCatalogRelation.class);
        List<Slot> mockLazySlots = ImmutableList.of(lazySlot);
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockCatalogRelation, mockRowIdSlot, mockLazySlots);

        // Return a different child to trigger hasNewChildren = true
        Plan modifiedRightChild = Mockito.mock(Plan.class);
        Mockito.when(mockRightChild.accept(Mockito.any(LazySlotPruning.class), Mockito.any(LazySlotPruning.Context.class))).thenReturn(modifiedRightChild);
        Mockito.when(modifiedJoin.child(1)).thenReturn(modifiedRightChild);

        lazySlotPruning.visitAbstractPhysicalJoin(mockJoin, context);

        java.lang.reflect.Field rowIdSlotField = LazySlotPruning.Context.class.getDeclaredField("rowIdSlot");
        rowIdSlotField.setAccessible(true);
        SlotReference updatedRowIdSlot = (SlotReference) rowIdSlotField.get(context);
        Assertions.assertTrue(updatedRowIdSlot.nullable());
    }

    @Test
    public void testVisitAbstractPhysicalJoinWithInnerJoinNoNullableUpdate() throws NoSuchFieldException, IllegalAccessException {
        AbstractPhysicalJoin mockJoin = Mockito.mock(AbstractPhysicalJoin.class);
        AbstractPhysicalPlan mockLeftChild = Mockito.mock(AbstractPhysicalPlan.class);
        AbstractPhysicalPlan mockRightChild = Mockito.mock(AbstractPhysicalPlan.class);

        SlotReference lazySlot = new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, false, ImmutableList.of());

        Mockito.when(mockLeftChild.getOutputSet()).thenReturn(ImmutableSet.of(lazySlot));
        Mockito.when(mockRightChild.getOutputSet()).thenReturn(ImmutableSet.of());
        Mockito.when(mockJoin.arity()).thenReturn(2);
        Mockito.when(mockJoin.child(0)).thenReturn(mockLeftChild);
        Mockito.when(mockJoin.child(1)).thenReturn(mockRightChild);
        Mockito.when(mockJoin.getJoinType()).thenReturn(JoinType.INNER_JOIN);

        AbstractPhysicalPlan modifiedJoin = Mockito.mock(AbstractPhysicalPlan.class);
        Mockito.when(mockJoin.withChildren(Mockito.<List<Plan>>any())).thenReturn(modifiedJoin);
        Mockito.when(modifiedJoin.copyStatsAndGroupIdFrom(mockJoin)).thenReturn(modifiedJoin);
        Mockito.when(modifiedJoin.resetLogicalProperties()).thenReturn(modifiedJoin);
        Mockito.when(modifiedJoin.getOutput()).thenReturn(ImmutableList.of(lazySlot, mockRowIdSlot));
        Mockito.when(modifiedJoin.child(0)).thenReturn(mockLeftChild);
        Mockito.when(modifiedJoin.child(1)).thenReturn(mockRightChild);

        PhysicalCatalogRelation mockCatalogRelation = Mockito.mock(PhysicalCatalogRelation.class);
        List<Slot> mockLazySlots = ImmutableList.of(lazySlot);
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockCatalogRelation, mockRowIdSlot, mockLazySlots);

        Mockito.when(mockLeftChild.accept(Mockito.any(LazySlotPruning.class), Mockito.any(LazySlotPruning.Context.class))).thenReturn(mockLeftChild);

        lazySlotPruning.visitAbstractPhysicalJoin(mockJoin, context);

        java.lang.reflect.Field rowIdSlotField = LazySlotPruning.Context.class.getDeclaredField("rowIdSlot");
        rowIdSlotField.setAccessible(true);
        SlotReference updatedRowIdSlot = (SlotReference) rowIdSlotField.get(context);
        // Inner join should not update nullable
        Assertions.assertFalse(updatedRowIdSlot.nullable());
    }

    @Test
    public void testVisitAbstractPhysicalJoinWithLazySlotsInChildAndChange() {
        AbstractPhysicalJoin mockJoin = Mockito.mock(AbstractPhysicalJoin.class);
        AbstractPhysicalPlan mockLeftChild = Mockito.mock(AbstractPhysicalPlan.class);
        Plan mockRightChild = Mockito.mock(Plan.class);

        SlotReference lazySlot = new SlotReference(new ExprId(2), "lazy1", IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference mockRowIdSlot = new SlotReference(new ExprId(1), "rowid", StringType.INSTANCE, false, ImmutableList.of());

        Mockito.when(mockLeftChild.getOutputSet()).thenReturn(ImmutableSet.of(lazySlot));
        Mockito.when(mockRightChild.getOutputSet()).thenReturn(ImmutableSet.of());
        Mockito.when(mockJoin.arity()).thenReturn(2);
        Mockito.when(mockJoin.child(0)).thenReturn(mockLeftChild);
        Mockito.when(mockJoin.child(1)).thenReturn(mockRightChild);
        Mockito.when(mockJoin.getJoinType()).thenReturn(JoinType.INNER_JOIN);

        Plan modifiedLeftChild = Mockito.mock(Plan.class);
        Mockito.when(mockLeftChild.accept(Mockito.any(LazySlotPruning.class), Mockito.any(LazySlotPruning.Context.class))).thenReturn(modifiedLeftChild);

        AbstractPhysicalPlan modifiedJoin = Mockito.mock(AbstractPhysicalPlan.class);
        Mockito.when(mockJoin.withChildren(Mockito.<List<Plan>>any())).thenReturn(modifiedJoin);
        Mockito.when(modifiedJoin.copyStatsAndGroupIdFrom(mockJoin)).thenReturn(modifiedJoin);
        Mockito.when(modifiedJoin.resetLogicalProperties()).thenReturn(modifiedJoin);
        Mockito.when(modifiedJoin.getOutput()).thenReturn(ImmutableList.of(lazySlot, mockRowIdSlot));
        Mockito.when(modifiedJoin.child(0)).thenReturn(modifiedLeftChild);
        Mockito.when(modifiedJoin.child(1)).thenReturn(mockRightChild);

        PhysicalCatalogRelation mockCatalogRelation = Mockito.mock(PhysicalCatalogRelation.class);
        List<Slot> mockLazySlots = ImmutableList.of(lazySlot);
        LazySlotPruning.Context context = new LazySlotPruning.Context(mockCatalogRelation, mockRowIdSlot, mockLazySlots);

        Plan result = lazySlotPruning.visitAbstractPhysicalJoin(mockJoin, context);

        Assertions.assertEquals(modifiedJoin, result);
        Mockito.verify(mockJoin, Mockito.times(1)).withChildren(Mockito.<List<Plan>>any());
    }
}
