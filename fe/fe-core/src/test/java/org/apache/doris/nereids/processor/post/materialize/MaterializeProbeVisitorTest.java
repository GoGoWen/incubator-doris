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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HiveTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Method;
import java.util.Optional;

public class MaterializeProbeVisitorTest {

    private MaterializeProbeVisitor visitor;
    @Mock private Plan mockPlan;
    @Mock private Plan mockChild1;
    @Mock private Plan mockChild2;
    @Mock private PhysicalOlapScan mockOlapScan;
    @Mock private PhysicalCatalogRelation mockCatalogRelation;
    @Mock private PhysicalLazyMaterialize mockLazyMaterialize;
    @Mock private PhysicalSetOperation mockSetOperation;
    @Mock private PhysicalProject mockProject;
    @Mock private HiveTable mockHiveTable;
    @Mock private OlapTable mockOlapTable;
    @Mock private IcebergExternalTable mockIcebergTable;
    @Mock private HMSExternalTable mockHmsTable;
    @Mock private Column mockColumn;

    private SlotReference testSlot;
    private SlotReference testSlot2;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        visitor = new MaterializeProbeVisitor();
        // Use spy to allow mocking getColumn() method
        testSlot = Mockito.spy(new SlotReference("col1", IntegerType.INSTANCE, true));
        testSlot2 = Mockito.spy(new SlotReference("col2", IntegerType.INSTANCE, true));
    }

    @Test
    public void testProbeContextConstructor() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Assertions.assertEquals(testSlot, context.slot);
    }

    @Test
    public void testVisitWithSlotInInputSlots() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Mockito.when(mockPlan.getInputSlots()).thenReturn(ImmutableSet.of(testSlot));

        Optional<MaterializeSource> result = visitor.visit(mockPlan, context);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testVisitWithSlotInChildOutput() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Mockito.when(mockPlan.getInputSlots()).thenReturn(ImmutableSet.of());
        Mockito.when(mockPlan.children()).thenReturn(ImmutableList.of(mockChild1, mockChild2));
        Mockito.when(mockChild1.getOutput()).thenReturn(ImmutableList.of());
        Mockito.when(mockChild2.getOutput()).thenReturn(ImmutableList.of(testSlot));
        Mockito.when(mockChild2.accept(Mockito.any(MaterializeProbeVisitor.class), Mockito.eq(context)))
                .thenReturn(Optional.of(new MaterializeSource(mockCatalogRelation, testSlot)));

        Optional<MaterializeSource> result = visitor.visit(mockPlan, context);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(mockCatalogRelation, result.get().relation);
        Assertions.assertEquals(testSlot, result.get().baseSlot);
    }

    @Test
    public void testVisitWithSlotNotInAnyChildOutput() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Mockito.when(mockPlan.getInputSlots()).thenReturn(ImmutableSet.of());
        Mockito.when(mockPlan.children()).thenReturn(ImmutableList.of(mockChild1, mockChild2));
        Mockito.when(mockChild1.getOutput()).thenReturn(ImmutableList.of());
        Mockito.when(mockChild2.getOutput()).thenReturn(ImmutableList.of());

        Optional<MaterializeSource> result = visitor.visit(mockPlan, context);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testVisitWithNoChildren() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Mockito.when(mockPlan.getInputSlots()).thenReturn(ImmutableSet.of());
        Mockito.when(mockPlan.children()).thenReturn(ImmutableList.of());

        Optional<MaterializeSource> result = visitor.visit(mockPlan, context);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testCheckRelationTableSupportedTypeWithHiveTable() throws Exception {
        Mockito.when(mockCatalogRelation.getTable()).thenReturn(mockHiveTable);
        Method method = MaterializeProbeVisitor.class.getDeclaredMethod(
                "checkRelationTableSupportedType", PhysicalCatalogRelation.class);
        method.setAccessible(true);
        boolean result = (boolean) method.invoke(visitor, mockCatalogRelation);
        Assertions.assertTrue(result);
    }

    @Test
    public void testCheckRelationTableSupportedTypeWithIcebergExternalTable() throws Exception {
        Mockito.when(mockCatalogRelation.getTable()).thenReturn(mockIcebergTable);
        Method method = MaterializeProbeVisitor.class.getDeclaredMethod(
                "checkRelationTableSupportedType", PhysicalCatalogRelation.class);
        method.setAccessible(true);
        boolean result = (boolean) method.invoke(visitor, mockCatalogRelation);
        Assertions.assertTrue(result);
    }

    @Test
    public void testCheckRelationTableSupportedTypeWithHMSExternalTableHive() throws Exception {
        Mockito.when(mockCatalogRelation.getTable()).thenReturn(mockHmsTable);
        Mockito.when(mockHmsTable.getDlaType()).thenReturn(DLAType.HIVE);
        Mockito.when(mockHmsTable.supportedHiveTopNLazyTable()).thenReturn(true);
        Method method = MaterializeProbeVisitor.class.getDeclaredMethod(
                "checkRelationTableSupportedType", PhysicalCatalogRelation.class);
        method.setAccessible(true);
        boolean result = (boolean) method.invoke(visitor, mockCatalogRelation);
        Assertions.assertTrue(result);
    }

    @Test
    public void testCheckRelationTableSupportedTypeWithHMSExternalTableHiveNotSupported() throws Exception {
        Mockito.when(mockCatalogRelation.getTable()).thenReturn(mockHmsTable);
        Mockito.when(mockHmsTable.getDlaType()).thenReturn(DLAType.HIVE);
        Mockito.when(mockHmsTable.supportedHiveTopNLazyTable()).thenReturn(false);
        Method method = MaterializeProbeVisitor.class.getDeclaredMethod(
                "checkRelationTableSupportedType", PhysicalCatalogRelation.class);
        method.setAccessible(true);
        boolean result = (boolean) method.invoke(visitor, mockCatalogRelation);
        Assertions.assertFalse(result);
    }

    @Test
    public void testCheckRelationTableSupportedTypeWithHMSExternalTableIceberg() throws Exception {
        Mockito.when(mockCatalogRelation.getTable()).thenReturn(mockHmsTable);
        Mockito.when(mockHmsTable.getDlaType()).thenReturn(DLAType.ICEBERG);
        Method method = MaterializeProbeVisitor.class.getDeclaredMethod(
                "checkRelationTableSupportedType", PhysicalCatalogRelation.class);
        method.setAccessible(true);
        boolean result = (boolean) method.invoke(visitor, mockCatalogRelation);
        Assertions.assertTrue(result);
    }

    @Test
    public void testCheckRelationTableSupportedTypeWithUnsupportedTable() throws Exception {
        org.apache.doris.catalog.Table unsupportedTable = Mockito.mock(org.apache.doris.catalog.Table.class);
        Mockito.when(mockCatalogRelation.getTable()).thenReturn(unsupportedTable);
        Method method = MaterializeProbeVisitor.class.getDeclaredMethod(
                "checkRelationTableSupportedType", PhysicalCatalogRelation.class);
        method.setAccessible(true);
        boolean result = (boolean) method.invoke(visitor, mockCatalogRelation);
        Assertions.assertFalse(result);
    }

    @Test
    public void testVisitPhysicalOlapScanWithBaseIndexId() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Mockito.when(mockOlapScan.getSelectedIndexId()).thenReturn(1L);
        Mockito.when(mockOlapScan.getTable()).thenReturn(mockOlapTable);
        Mockito.when(mockOlapTable.getBaseIndexId()).thenReturn(1L);
        Mockito.when(mockOlapScan.getOutput()).thenReturn(ImmutableList.of(testSlot));
        Mockito.when(mockOlapScan.getOperativeSlots()).thenReturn(ImmutableList.of());
        Mockito.when(testSlot.getColumn()).thenReturn(Optional.of(mockColumn));

        Optional<MaterializeSource> result = visitor.visitPhysicalOlapScan(mockOlapScan, context);
        // Since checkRelationTableSupportedType will be called and OlapTable is not in supported types,
        // result should be empty. However, visitPhysicalOlapScan calls visitPhysicalCatalogRelation
        // which checks the table type, so it will return empty for OlapTable.
        // Actually, PhysicalOlapScan extends PhysicalCatalogRelation, so it will check the table type.
        // Since OlapTable is not in SUPPORT_RELATION_TYPES, it should return empty.
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testVisitPhysicalOlapScanWithNonBaseIndexId() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Mockito.when(mockOlapScan.getSelectedIndexId()).thenReturn(1L);
        Mockito.when(mockOlapScan.getTable()).thenReturn(mockOlapTable);
        Mockito.when(mockOlapTable.getBaseIndexId()).thenReturn(2L);

        Optional<MaterializeSource> result = visitor.visitPhysicalOlapScan(mockOlapScan, context);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testVisitPhysicalCatalogRelationWithAllConditionsMet() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Mockito.when(mockCatalogRelation.getTable()).thenReturn(mockHiveTable);
        Mockito.when(mockCatalogRelation.getOutput()).thenReturn(ImmutableList.of(testSlot));
        Mockito.when(mockCatalogRelation.getOperativeSlots()).thenReturn(ImmutableList.of());
        Mockito.when(testSlot.getColumn()).thenReturn(Optional.of(mockColumn));

        Optional<MaterializeSource> result = visitor.visitPhysicalCatalogRelation(mockCatalogRelation, context);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(mockCatalogRelation, result.get().relation);
        Assertions.assertEquals(testSlot, result.get().baseSlot);
    }

    @Test
    public void testVisitPhysicalCatalogRelationWithSlotNotInOutput() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Mockito.when(mockCatalogRelation.getTable()).thenReturn(mockHiveTable);
        Mockito.when(mockCatalogRelation.getOutput()).thenReturn(ImmutableList.of(testSlot2));
        Mockito.when(mockCatalogRelation.getOperativeSlots()).thenReturn(ImmutableList.of());

        Optional<MaterializeSource> result = visitor.visitPhysicalCatalogRelation(mockCatalogRelation, context);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testVisitPhysicalCatalogRelationWithSlotInOperativeSlots() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Mockito.when(mockCatalogRelation.getTable()).thenReturn(mockHiveTable);
        Mockito.when(mockCatalogRelation.getOutput()).thenReturn(ImmutableList.of(testSlot));
        Mockito.when(mockCatalogRelation.getOperativeSlots()).thenReturn(ImmutableList.of(testSlot));

        Optional<MaterializeSource> result = visitor.visitPhysicalCatalogRelation(mockCatalogRelation, context);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testVisitPhysicalCatalogRelationWithNoColumn() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Mockito.when(mockCatalogRelation.getTable()).thenReturn(mockHiveTable);
        Mockito.when(mockCatalogRelation.getOutput()).thenReturn(ImmutableList.of(testSlot));
        Mockito.when(mockCatalogRelation.getOperativeSlots()).thenReturn(ImmutableList.of());
        Mockito.when(testSlot.getColumn()).thenReturn(Optional.empty());

        Optional<MaterializeSource> result = visitor.visitPhysicalCatalogRelation(mockCatalogRelation, context);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testVisitPhysicalCatalogRelationWithUnsupportedTableType() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        org.apache.doris.catalog.Table unsupportedTable = Mockito.mock(org.apache.doris.catalog.Table.class);
        Mockito.when(mockCatalogRelation.getTable()).thenReturn(unsupportedTable);
        Mockito.when(mockCatalogRelation.getOutput()).thenReturn(ImmutableList.of(testSlot));
        Mockito.when(mockCatalogRelation.getOperativeSlots()).thenReturn(ImmutableList.of());
        Mockito.when(testSlot.getColumn()).thenReturn(Optional.of(mockColumn));

        Optional<MaterializeSource> result = visitor.visitPhysicalCatalogRelation(mockCatalogRelation, context);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testVisitPhysicalLazyMaterialize() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Plan mockChild = Mockito.mock(Plan.class);
        Mockito.when(mockLazyMaterialize.child()).thenReturn(mockChild);
        Mockito.when(mockChild.accept(Mockito.any(MaterializeProbeVisitor.class), Mockito.eq(context)))
                .thenReturn(Optional.of(new MaterializeSource(mockCatalogRelation, testSlot)));

        Optional<MaterializeSource> result = visitor.visitPhysicalLazyMaterialize(mockLazyMaterialize, context);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(mockCatalogRelation, result.get().relation);
    }

    @Test
    public void testVisitPhysicalSetOperation() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Optional<MaterializeSource> result = visitor.visitPhysicalSetOperation(mockSetOperation, context);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testVisitPhysicalProjectWithSlotNotInOutput() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Mockito.when(mockProject.getOutput()).thenReturn(ImmutableList.of(testSlot2));

        Optional<MaterializeSource> result = visitor.visitPhysicalProject(mockProject, context);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testVisitPhysicalProjectWithSlotReference() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Plan mockChild = Mockito.mock(Plan.class);
        Mockito.when(mockProject.getOutput()).thenReturn(ImmutableList.of(testSlot));
        Mockito.when(mockProject.getProjects()).thenReturn(ImmutableList.of((NamedExpression) testSlot));
        Mockito.when(mockProject.child()).thenReturn(mockChild);
        Mockito.when(mockChild.accept(Mockito.any(MaterializeProbeVisitor.class), Mockito.eq(context)))
                .thenReturn(Optional.of(new MaterializeSource(mockCatalogRelation, testSlot)));

        Optional<MaterializeSource> result = visitor.visitPhysicalProject(mockProject, context);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(mockCatalogRelation, result.get().relation);
    }

    @Test
    public void testVisitPhysicalProjectWithAliasAndSlotReferenceChild() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Plan mockChild = Mockito.mock(Plan.class);
        Alias alias = new Alias(testSlot2, "alias1");
        Mockito.when(mockProject.getOutput()).thenReturn(ImmutableList.of(testSlot));
        Mockito.when(mockProject.getProjects()).thenReturn(ImmutableList.of((NamedExpression) alias));
        Mockito.when(mockProject.child()).thenReturn(mockChild);
        Mockito.when(mockChild.accept(Mockito.any(MaterializeProbeVisitor.class), Mockito.any(MaterializeProbeVisitor.ProbeContext.class)))
                .thenReturn(Optional.of(new MaterializeSource(mockCatalogRelation, testSlot2)));

        Optional<MaterializeSource> result = visitor.visitPhysicalProject(mockProject, context);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(mockCatalogRelation, result.get().relation);
        Assertions.assertEquals(testSlot2, result.get().baseSlot);
    }

    @Test
    public void testVisitPhysicalProjectWithAliasAndNonSlotReferenceChild() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        IntegerLiteral literal = new IntegerLiteral(1);
        Alias alias = new Alias(literal, "alias1");
        Mockito.when(mockProject.getOutput()).thenReturn(ImmutableList.of(testSlot));
        Mockito.when(mockProject.getProjects()).thenReturn(ImmutableList.of((NamedExpression) alias));

        Optional<MaterializeSource> result = visitor.visitPhysicalProject(mockProject, context);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testVisitPhysicalProjectWithEmptyOutput() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Mockito.when(mockProject.getOutput()).thenReturn(ImmutableList.of());

        Optional<MaterializeSource> result = visitor.visitPhysicalProject(mockProject, context);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testVisitWithMultipleChildrenAndSlotInFirstChild() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Plan mockChild3 = Mockito.mock(Plan.class);
        Mockito.when(mockPlan.getInputSlots()).thenReturn(ImmutableSet.of());
        Mockito.when(mockPlan.children()).thenReturn(ImmutableList.of(mockChild1, mockChild2, mockChild3));
        Mockito.when(mockChild1.getOutput()).thenReturn(ImmutableList.of(testSlot));
        Mockito.when(mockChild2.getOutput()).thenReturn(ImmutableList.of(testSlot2));
        Mockito.when(mockChild3.getOutput()).thenReturn(ImmutableList.of());
        Mockito.when(mockChild1.accept(Mockito.any(MaterializeProbeVisitor.class), Mockito.eq(context)))
                .thenReturn(Optional.of(new MaterializeSource(mockCatalogRelation, testSlot)));

        Optional<MaterializeSource> result = visitor.visit(mockPlan, context);
        Assertions.assertTrue(result.isPresent());
        // Should use first child that contains the slot
        Mockito.verify(mockChild1, Mockito.times(1))
                .accept(Mockito.any(MaterializeProbeVisitor.class), Mockito.eq(context));
        Mockito.verify(mockChild2, Mockito.never())
                .accept(Mockito.any(MaterializeProbeVisitor.class), Mockito.any());
    }

    @Test
    public void testVisitPhysicalCatalogRelationWithHMSExternalTableIcebergType() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Mockito.when(mockCatalogRelation.getTable()).thenReturn(mockHmsTable);
        Mockito.when(mockHmsTable.getDlaType()).thenReturn(DLAType.ICEBERG);
        Mockito.when(mockCatalogRelation.getOutput()).thenReturn(ImmutableList.of(testSlot));
        Mockito.when(mockCatalogRelation.getOperativeSlots()).thenReturn(ImmutableList.of());
        Mockito.when(testSlot.getColumn()).thenReturn(Optional.of(mockColumn));

        Optional<MaterializeSource> result = visitor.visitPhysicalCatalogRelation(mockCatalogRelation, context);
        Assertions.assertTrue(result.isPresent());
    }

    @Test
    public void testVisitPhysicalCatalogRelationWithHMSExternalTableHiveTypeSupported() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Mockito.when(mockCatalogRelation.getTable()).thenReturn(mockHmsTable);
        Mockito.when(mockHmsTable.getDlaType()).thenReturn(DLAType.HIVE);
        Mockito.when(mockHmsTable.supportedHiveTopNLazyTable()).thenReturn(true);
        Mockito.when(mockCatalogRelation.getOutput()).thenReturn(ImmutableList.of(testSlot));
        Mockito.when(mockCatalogRelation.getOperativeSlots()).thenReturn(ImmutableList.of());
        Mockito.when(testSlot.getColumn()).thenReturn(Optional.of(mockColumn));

        Optional<MaterializeSource> result = visitor.visitPhysicalCatalogRelation(mockCatalogRelation, context);
        Assertions.assertTrue(result.isPresent());
    }

    @Test
    public void testVisitPhysicalCatalogRelationWithHMSExternalTableHiveTypeNotSupported() {
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(testSlot);
        Mockito.when(mockCatalogRelation.getTable()).thenReturn(mockHmsTable);
        Mockito.when(mockHmsTable.getDlaType()).thenReturn(DLAType.HIVE);
        Mockito.when(mockHmsTable.supportedHiveTopNLazyTable()).thenReturn(false);
        Mockito.when(mockCatalogRelation.getOutput()).thenReturn(ImmutableList.of(testSlot));
        Mockito.when(mockCatalogRelation.getOperativeSlots()).thenReturn(ImmutableList.of());
        Mockito.when(testSlot.getColumn()).thenReturn(Optional.of(mockColumn));

        Optional<MaterializeSource> result = visitor.visitPhysicalCatalogRelation(mockCatalogRelation, context);
        Assertions.assertFalse(result.isPresent());
    }
}

