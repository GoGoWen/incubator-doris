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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.processor.post.materialize.MaterializeSource;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PhysicalLazyMaterializeTest {

    @Mock private Plan mockChildPlan;
    @Mock private PhysicalProperties mockPhysicalProperties;
    @Mock private Statistics mockStatistics;
    @Mock private LogicalProperties mockLogicalProperties;
    @Mock private GroupExpression mockGroupExpression;
    @Mock private CatalogRelation mockCatalogRelation;
    @Mock private TableIf mockTableIf;

    @Mock private SlotReference rowIdSlot1; // Mocked SlotReference
    @Mock private SlotReference lazySlot1; // Mocked SlotReference
    @Mock private SlotReference baseSlot1; // Mocked SlotReference
    @Mock private Column baseColumn1; // Mocked Column

    private List<Slot> materializeInput;
    private List<Slot> materializedSlots;
    private Map<CatalogRelation, List<Slot>> relationToLazySlotMap;
    private BiMap<CatalogRelation, SlotReference> relationToRowId;
    private Map<Slot, MaterializeSource> materializeMap;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // 先创建真实的 SlotReference 用于 materializedSlots
        materializedSlots = ImmutableList.of(
            new SlotReference("mat_col_1", StringType.INSTANCE, false),
            new SlotReference("mat_col_2", IntegerType.INSTANCE, false)
        );

        // Mock SlotReference behaviors
        // 确保 rowIdSlot1 在 relationToRowId 中
        Mockito.when(rowIdSlot1.getName()).thenReturn("row_id_1");
        Mockito.when(rowIdSlot1.getDataType()).thenReturn(StringType.INSTANCE);
        Mockito.when(rowIdSlot1.getExprId()).thenReturn(new ExprId(10));
        Mockito.when(rowIdSlot1.getColumn()).thenReturn(Optional.empty()); // rowId 可能没有column

        // 确保 lazySlot1 在 relationToLazySlotMap 和 materializeMap 中
        Mockito.when(lazySlot1.getName()).thenReturn("lazy_col_1");
        Mockito.when(lazySlot1.getDataType()).thenReturn(IntegerType.INSTANCE);
        Mockito.when(lazySlot1.getExprId()).thenReturn(new ExprId(11));
        Mockito.when(lazySlot1.getColumn()).thenReturn(Optional.of(baseColumn1));

        Mockito.when(baseSlot1.getName()).thenReturn("base_col_1");
        Mockito.when(baseSlot1.getDataType()).thenReturn(IntegerType.INSTANCE);
        Mockito.when(baseSlot1.getExprId()).thenReturn(new ExprId(12));
        Mockito.when(baseSlot1.getColumn()).thenReturn(Optional.of(baseColumn1));

        // Mock Column
        Mockito.when(baseColumn1.getName()).thenReturn("base_col_1");
        Mockito.when(baseColumn1.getDataType()).thenReturn(org.apache.doris.catalog.PrimitiveType.INT);

        // 创建 MaterializeSource - 注意：构造函数需要 baseSlot，它应该是一个真实的 SlotReference
        // 由于 baseSlot1 是 Mock，我们需要确保它能正常工作
        MaterializeSource mockMaterializeSource1 = new MaterializeSource(mockCatalogRelation, baseSlot1);

        // Setup lists and maps for constructor
        // materializeInput 应该包含 materializedSlots + rowId
        // lazySlot 不应该在 materializeInput 中，它在 relationToLazySlotMap 中
        materializeInput = ImmutableList.<Slot>builder()
                .addAll(materializedSlots)
                .add(rowIdSlot1)
                .build();

        relationToLazySlotMap = ImmutableMap.of(
                mockCatalogRelation, ImmutableList.of(lazySlot1)
        );

        relationToRowId = HashBiMap.create();
        relationToRowId.put(mockCatalogRelation, rowIdSlot1);

        materializeMap = ImmutableMap.of(
                lazySlot1, mockMaterializeSource1
        );

        // Mock CatalogRelation and TableIf behavior
        Mockito.when(mockCatalogRelation.getTable()).thenReturn(mockTableIf);
        Mockito.when(mockTableIf.getName()).thenReturn("test_table");
        Mockito.when(mockTableIf.getBaseColumnIdxByName("lazy_col_1")).thenReturn(0);

        // Mock LogicalProperties.getOutput()
        Mockito.when(mockLogicalProperties.getOutput()).thenReturn(ImmutableList.of());        // 确保 mockChildPlan 有基本的方法
        Mockito.when(mockChildPlan.children()).thenReturn(ImmutableList.of());
        Mockito.when(mockChildPlan.getOutput()).thenReturn(ImmutableList.of());

        // 设置 statistics 和 physicalProperties
        Mockito.when(mockStatistics.getRowCount()).thenReturn(1000.0);
    }

    @Test
    public void testConstructorMinimal() {
        // Test the constructor with only required arguments
        PhysicalLazyMaterialize lazyMaterialize = new PhysicalLazyMaterialize(
                mockChildPlan, ImmutableList.of(), ImmutableList.of(), ImmutableMap.of(), HashBiMap.create(), ImmutableMap.of());

        Assertions.assertNotNull(lazyMaterialize);
        Assertions.assertEquals(mockChildPlan, lazyMaterialize.child());
        Assertions.assertTrue(lazyMaterialize.getExpressions().isEmpty());
        Assertions.assertTrue(lazyMaterialize.getRelations().isEmpty());
        Assertions.assertTrue(lazyMaterialize.getLazyColumns().isEmpty());
        Assertions.assertTrue(lazyMaterialize.getLazySlotLocations().isEmpty());
        Assertions.assertTrue(lazyMaterialize.getlazyTableIdxs().isEmpty());
        Assertions.assertTrue(lazyMaterialize.getRowIds().isEmpty());
        Assertions.assertTrue(lazyMaterialize.computeOutput().isEmpty());
    }

    @Test
    public void testConstructorFull() {
        PhysicalLazyMaterialize lazyMaterialize = new PhysicalLazyMaterialize(
                mockChildPlan, materializeInput, materializedSlots, relationToLazySlotMap,
                relationToRowId, materializeMap, mockPhysicalProperties, mockStatistics);

        Assertions.assertNotNull(lazyMaterialize);
        Assertions.assertEquals(mockChildPlan, lazyMaterialize.child());
        Assertions.assertEquals(materializedSlots, lazyMaterialize.getExpressions());
        Assertions.assertEquals(ImmutableList.of(mockCatalogRelation), lazyMaterialize.getRelations());
        Assertions.assertEquals(mockStatistics, lazyMaterialize.getStats());
        Assertions.assertEquals(mockPhysicalProperties, lazyMaterialize.getPhysicalProperties());

        // Verify computed lists
        Assertions.assertEquals(ImmutableList.of(baseColumn1), lazyMaterialize.getLazyColumns().get(0));
        Assertions.assertEquals(ImmutableList.of(0), lazyMaterialize.getlazyTableIdxs().get(0));
        Assertions.assertEquals(ImmutableList.of(rowIdSlot1), lazyMaterialize.getRowIds());

        List<Slot> expectedOutput = ImmutableList.<Slot>builder()
                .addAll(materializedSlots)
                .add(lazySlot1) // Lazy slot is added after rowId, but then order by loc
                .build();
        Assertions.assertEquals(expectedOutput, lazyMaterialize.computeOutput());
    }

    @Test
    public void testGetters() {
        PhysicalLazyMaterialize lazyMaterialize = new PhysicalLazyMaterialize(
                mockChildPlan, materializeInput, materializedSlots, relationToLazySlotMap,
                relationToRowId, materializeMap, mockPhysicalProperties, mockStatistics);

        Assertions.assertEquals(materializedSlots, lazyMaterialize.getExpressions());
        Assertions.assertEquals(ImmutableList.of(mockCatalogRelation), lazyMaterialize.getRelations());
        Assertions.assertEquals(mockStatistics, lazyMaterialize.getStats());
        Assertions.assertEquals(mockPhysicalProperties, lazyMaterialize.getPhysicalProperties());
        Assertions.assertEquals(ImmutableList.of(baseColumn1), lazyMaterialize.getLazyColumns().get(0));
        Assertions.assertEquals(ImmutableList.of(0), lazyMaterialize.getlazyTableIdxs().get(0));
        Assertions.assertEquals(ImmutableList.of(rowIdSlot1), lazyMaterialize.getRowIds());
    }

    @Test
    public void testWithChildren() {
        PhysicalLazyMaterialize originalLazyMaterialize = new PhysicalLazyMaterialize(
                mockChildPlan, materializeInput, materializedSlots, relationToLazySlotMap,
                relationToRowId, materializeMap, mockPhysicalProperties, mockStatistics);

        Plan newChildPlan = Mockito.mock(Plan.class);
        List<Plan> newChildren = ImmutableList.of(newChildPlan);

        PhysicalLazyMaterialize newLazyMaterialize = (PhysicalLazyMaterialize) originalLazyMaterialize.withChildren(newChildren);

        Assertions.assertNotEquals(originalLazyMaterialize, newLazyMaterialize);
        Assertions.assertEquals(newChildPlan, newLazyMaterialize.child());
        Assertions.assertEquals(originalLazyMaterialize.getExpressions(), newLazyMaterialize.getExpressions());
        Assertions.assertEquals(originalLazyMaterialize.getRelations(), newLazyMaterialize.getRelations());
        Assertions.assertNull(newLazyMaterialize.getStats()); // withChildren creates new with null statistics
    }

    @Test
    public void testWithPhysicalPropertiesAndStats() {
        PhysicalLazyMaterialize originalLazyMaterialize = new PhysicalLazyMaterialize(
                mockChildPlan, materializeInput, materializedSlots, relationToLazySlotMap,
                relationToRowId, materializeMap, null, null);

        PhysicalProperties newPhysicalProperties = Mockito.mock(PhysicalProperties.class);
        Statistics newStatistics = Mockito.mock(Statistics.class);

        PhysicalLazyMaterialize newLazyMaterialize = (PhysicalLazyMaterialize) originalLazyMaterialize.withPhysicalPropertiesAndStats(
                newPhysicalProperties, newStatistics);

        Assertions.assertNotEquals(originalLazyMaterialize, newLazyMaterialize);
        Assertions.assertEquals(mockChildPlan, newLazyMaterialize.child());
        Assertions.assertEquals(newPhysicalProperties, newLazyMaterialize.getPhysicalProperties());
        Assertions.assertEquals(newStatistics, newLazyMaterialize.getStats());
        Assertions.assertEquals(originalLazyMaterialize.getExpressions(), newLazyMaterialize.getExpressions());
    }

    @Test
    public void testToStringAndShapeInfo() {
        PhysicalLazyMaterialize lazyMaterialize = new PhysicalLazyMaterialize(
                mockChildPlan, materializeInput, materializedSlots, relationToLazySlotMap,
                relationToRowId, materializeMap, mockPhysicalProperties, mockStatistics);

        // Test shapeInfo
        String shapeInfoResult = lazyMaterialize.shapeInfo();
        Assertions.assertNotNull(shapeInfoResult);
        Assertions.assertTrue(shapeInfoResult.contains("PhysicalLazyMaterialize"));
        Assertions.assertTrue(shapeInfoResult.contains("materializedSlots:"));
        Assertions.assertTrue(shapeInfoResult.contains("lazySlots:"));
    }
}
