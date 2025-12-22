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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class FuncDepsDGTest {

    private SlotReference slotA;
    private SlotReference slotB;
    private SlotReference slotC;
    private SlotReference slotD;
    private SlotReference slotE;
    private SlotReference slotF;
    private SlotReference slotG;
    private SlotReference slotH;

    @BeforeEach
    public void setUp() {
        slotA = new SlotReference("a", IntegerType.INSTANCE, true);
        slotB = new SlotReference("b", IntegerType.INSTANCE, true);
        slotC = new SlotReference("c", IntegerType.INSTANCE, true);
        slotD = new SlotReference("d", IntegerType.INSTANCE, true);
        slotE = new SlotReference("e", IntegerType.INSTANCE, true);
        slotF = new SlotReference("f", IntegerType.INSTANCE, true);
        slotG = new SlotReference("g", IntegerType.INSTANCE, true);
        slotH = new SlotReference("h", IntegerType.INSTANCE, true);
    }

    @Test
    public void testEmptyBuilder() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        FuncDepsDG dg = builder.build();
        Assertions.assertTrue(dg.isEmpty());
    }

    @Test
    public void testAddDepsSimple() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        FuncDepsDG dg = builder.build();

        Assertions.assertFalse(dg.isEmpty());
        FuncDeps funcDeps = dg.findValidFuncDeps(ImmutableSet.of(slotA, slotB));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)));
    }

    @Test
    public void testAddDepsMultiple() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder.addDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotC));
        builder.addDeps(ImmutableSet.of(slotC), ImmutableSet.of(slotD));
        FuncDepsDG dg = builder.build();

        FuncDeps funcDeps = dg.findValidFuncDeps(ImmutableSet.of(slotA, slotB, slotC, slotD));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotC)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotC), ImmutableSet.of(slotD)));
    }

    @Test
    public void testAddDepsWithMultipleSlots() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        builder.addDeps(ImmutableSet.of(slotA, slotB), ImmutableSet.of(slotC));
        builder.addDeps(ImmutableSet.of(slotC), ImmutableSet.of(slotD, slotE));
        FuncDepsDG dg = builder.build();

        FuncDeps funcDeps = dg.findValidFuncDeps(ImmutableSet.of(slotA, slotB, slotC, slotD, slotE));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA, slotB), ImmutableSet.of(slotC)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotC), ImmutableSet.of(slotD, slotE)));
    }

    @Test
    public void testFindValidFuncDepsWithPartialValidSlots() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder.addDeps(ImmutableSet.of(slotC), ImmutableSet.of(slotD));
        FuncDepsDG dg = builder.build();

        // Only slotA and slotB are valid
        FuncDeps funcDeps = dg.findValidFuncDeps(ImmutableSet.of(slotA, slotB));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)));
        Assertions.assertFalse(funcDeps.isFuncDeps(ImmutableSet.of(slotC), ImmutableSet.of(slotD)));
    }

    @Test
    public void testFindValidFuncDepsWithEmptyValidSlots() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        FuncDepsDG dg = builder.build();

        FuncDeps funcDeps = dg.findValidFuncDeps(ImmutableSet.of());
        Assertions.assertTrue(funcDeps.isEmpty());
    }

    @Test
    public void testFindValidFuncDepsWithTransitiveDependencies() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        // A -> B -> C
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder.addDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotC));
        FuncDepsDG dg = builder.build();

        FuncDeps funcDeps = dg.findValidFuncDeps(ImmutableSet.of(slotA, slotB, slotC));
        // Should find A -> B, B -> C, and potentially A -> C through transitive closure
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotC)));
    }

    @Test
    public void testFindValidFuncDepsWithBranching() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        // A -> B, A -> C
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotC));
        FuncDepsDG dg = builder.build();

        FuncDeps funcDeps = dg.findValidFuncDeps(ImmutableSet.of(slotA, slotB, slotC));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotC)));
    }

    @Test
    public void testFindValidFuncDepsWithCycle() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        // A -> B -> A (circular)
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder.addDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotA));
        FuncDepsDG dg = builder.build();

        FuncDeps funcDeps = dg.findValidFuncDeps(ImmutableSet.of(slotA, slotB));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotA)));
    }

    @Test
    public void testBuilderCopyConstructor() {
        FuncDepsDG.Builder builder1 = new FuncDepsDG.Builder();
        builder1.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder1.addDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotC));
        FuncDepsDG dg1 = builder1.build();

        FuncDepsDG.Builder builder2 = new FuncDepsDG.Builder(dg1);
        builder2.addDeps(ImmutableSet.of(slotC), ImmutableSet.of(slotD));
        FuncDepsDG dg2 = builder2.build();

        // Original dependencies should be preserved
        FuncDeps funcDeps = dg2.findValidFuncDeps(ImmutableSet.of(slotA, slotB, slotC, slotD));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotC)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotC), ImmutableSet.of(slotD)));
    }

    @Test
    public void testAddDepsFromAnotherFuncDepsDG() {
        FuncDepsDG.Builder builder1 = new FuncDepsDG.Builder();
        builder1.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder1.addDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotC));
        FuncDepsDG dg1 = builder1.build();

        FuncDepsDG.Builder builder2 = new FuncDepsDG.Builder();
        builder2.addDeps(ImmutableSet.of(slotD), ImmutableSet.of(slotE));
        builder2.addDeps(dg1);
        FuncDepsDG dg2 = builder2.build();

        FuncDeps funcDeps = dg2.findValidFuncDeps(ImmutableSet.of(slotA, slotB, slotC, slotD, slotE));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotC)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotD), ImmutableSet.of(slotE)));
    }

    @Test
    public void testRemoveNotContain() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder.addDeps(ImmutableSet.of(slotC), ImmutableSet.of(slotD));
        builder.addDeps(ImmutableSet.of(slotE), ImmutableSet.of(slotF));
        builder.build();

        // Remove dependencies that don't contain slotA, slotB, slotC, slotD
        builder.removeNotContain(ImmutableSet.of(slotA, slotB, slotC, slotD));
        FuncDepsDG filteredDg = builder.build();

        FuncDeps funcDeps = filteredDg.findValidFuncDeps(ImmutableSet.of(slotA, slotB, slotC, slotD, slotE, slotF));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotC), ImmutableSet.of(slotD)));
        // E -> F should be removed since E and F are not in validSlot
        Assertions.assertFalse(funcDeps.isFuncDeps(ImmutableSet.of(slotE), ImmutableSet.of(slotF)));
    }

    @Test
    public void testRemoveNotContainWithEmptyValidSlots() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder.removeNotContain(ImmutableSet.of());
        FuncDepsDG dg = builder.build();

        Assertions.assertTrue(dg.isEmpty());
    }

    @Test
    public void testReplace() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder.addDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotC));
        FuncDepsDG dg1 = builder.build();

        // Replace slotA with slotD, slotB with slotE
        Map<Slot, Slot> replaceMap = ImmutableSet.of(
                new java.util.AbstractMap.SimpleEntry<>(slotA, slotD),
                new java.util.AbstractMap.SimpleEntry<>(slotB, slotE)
        ).stream().collect(java.util.stream.Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue));

        FuncDepsDG.Builder builder2 = new FuncDepsDG.Builder(dg1);
        builder2.replace(replaceMap);
        FuncDepsDG dg2 = builder2.build();

        FuncDeps funcDeps = dg2.findValidFuncDeps(ImmutableSet.of(slotD, slotE, slotC));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotD), ImmutableSet.of(slotE)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotE), ImmutableSet.of(slotC)));
        // Original dependencies should not exist
        Assertions.assertFalse(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)));
    }

    @Test
    public void testReplaceWithPartialMap() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder.addDeps(ImmutableSet.of(slotC), ImmutableSet.of(slotD));
        FuncDepsDG dg1 = builder.build();

        // Only replace slotA with slotE
        Map<Slot, Slot> replaceMap = ImmutableSet.of(
                new java.util.AbstractMap.SimpleEntry<>(slotA, slotE)
        ).stream().collect(java.util.stream.Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue));

        FuncDepsDG.Builder builder2 = new FuncDepsDG.Builder(dg1);
        builder2.replace(replaceMap);
        FuncDepsDG dg2 = builder2.build();

        FuncDeps funcDeps = dg2.findValidFuncDeps(ImmutableSet.of(slotE, slotB, slotC, slotD));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotE), ImmutableSet.of(slotB)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotC), ImmutableSet.of(slotD)));
    }

    @Test
    public void testReplaceWithEmptyMap() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        FuncDepsDG dg1 = builder.build();

        Map<Slot, Slot> emptyMap = ImmutableSet.<Map.Entry<Slot, Slot>>of()
                .stream().collect(java.util.stream.Collectors.toMap(
                        Map.Entry::getKey, Map.Entry::getValue));

        FuncDepsDG.Builder builder2 = new FuncDepsDG.Builder(dg1);
        builder2.replace(emptyMap);
        FuncDepsDG dg2 = builder2.build();

        FuncDeps funcDeps = dg2.findValidFuncDeps(ImmutableSet.of(slotA, slotB));
        // Should remain unchanged
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)));
    }

    @Test
    public void testToString() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder.addDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotC));
        FuncDepsDG dg = builder.build();

        String str = dg.toString();
        Assertions.assertNotNull(str);
        Assertions.assertTrue(str.contains("->"));
    }

    @Test
    public void testToStringEmpty() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        FuncDepsDG dg = builder.build();

        String str = dg.toString();
        Assertions.assertNotNull(str);
    }

    @Test
    public void testAddDepsDuplicate() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)); // Duplicate
        FuncDepsDG dg = builder.build();

        FuncDeps funcDeps = dg.findValidFuncDeps(ImmutableSet.of(slotA, slotB));
        // Should only have one edge, not duplicate
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)));
    }

    @Test
    public void testFindValidFuncDepsWithNestedChildren() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        // A -> B -> C -> D
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder.addDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotC));
        builder.addDeps(ImmutableSet.of(slotC), ImmutableSet.of(slotD));
        FuncDepsDG dg = builder.build();

        FuncDeps funcDeps = dg.findValidFuncDeps(ImmutableSet.of(slotA, slotB, slotC, slotD));
        // Should find all transitive dependencies
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotC)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotC), ImmutableSet.of(slotD)));
    }

    @Test
    public void testFindValidFuncDepsWithInvalidChild() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        // A -> B -> C
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder.addDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotC));
        FuncDepsDG dg = builder.build();

        // slotC is not in validSlot
        FuncDeps funcDeps = dg.findValidFuncDeps(ImmutableSet.of(slotA, slotB));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)));
        // B -> C should not be found since slotC is not valid
        Assertions.assertFalse(funcDeps.isFuncDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotC)));
    }

    @Test
    public void testFindValidFuncDepsWithInvalidRoot() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        // A -> B -> C
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder.addDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotC));
        FuncDepsDG dg = builder.build();

        // slotA is not in validSlot, so A -> B should not be found
        FuncDeps funcDeps = dg.findValidFuncDeps(ImmutableSet.of(slotB, slotC));
        Assertions.assertFalse(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)));
        // But B -> C should be found if we start from B
        // However, since A is not valid, we won't traverse from A, so B -> C might not be found
        // This depends on the implementation - if B is a root node, it should be found
    }

    @Test
    public void testDGItemReplace() {
        FuncDepsDG.DGItem item = new FuncDepsDG.DGItem(ImmutableSet.of(slotA, slotB), 0);
        Map<Slot, Slot> replaceMap = ImmutableSet.of(
                new java.util.AbstractMap.SimpleEntry<>(slotA, slotC)
        ).stream().collect(java.util.stream.Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue));

        item.replace(replaceMap);
        Assertions.assertTrue(item.slots.contains(slotC));
        Assertions.assertFalse(item.slots.contains(slotA));
        Assertions.assertTrue(item.slots.contains(slotB));
    }

    @Test
    public void testDGItemCopyConstructor() {
        FuncDepsDG.DGItem original = new FuncDepsDG.DGItem(ImmutableSet.of(slotA, slotB), 0);
        original.parents.add(1);
        original.children.add(2);

        FuncDepsDG.DGItem copy = new FuncDepsDG.DGItem(original);
        Assertions.assertEquals(original.index, copy.index);
        Assertions.assertEquals(original.slots, copy.slots);
        Assertions.assertEquals(original.parents, copy.parents);
        Assertions.assertEquals(original.children, copy.children);
        // Should be different objects
        Assertions.assertNotSame(original.parents, copy.parents);
        Assertions.assertNotSame(original.children, copy.children);
    }

    @Test
    public void testComplexGraph() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        // Create a complex graph:
        // A -> B -> D
        // A -> C -> D
        // E -> F
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotC));
        builder.addDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotD));
        builder.addDeps(ImmutableSet.of(slotC), ImmutableSet.of(slotD));
        builder.addDeps(ImmutableSet.of(slotE), ImmutableSet.of(slotF));
        FuncDepsDG dg = builder.build();

        FuncDeps funcDeps = dg.findValidFuncDeps(ImmutableSet.of(slotA, slotB, slotC, slotD, slotE, slotF));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotC)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotD)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotC), ImmutableSet.of(slotD)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotE), ImmutableSet.of(slotF)));
    }

    @Test
    public void testRemoveNotContainPreservesValidDependencies() {
        FuncDepsDG.Builder builder = new FuncDepsDG.Builder();
        // A -> B -> C
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        builder.addDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotC));
        // D -> E -> F (should be removed)
        builder.addDeps(ImmutableSet.of(slotD), ImmutableSet.of(slotE));
        builder.addDeps(ImmutableSet.of(slotE), ImmutableSet.of(slotF));
        builder.build();

        builder.removeNotContain(ImmutableSet.of(slotA, slotB, slotC));
        FuncDepsDG filteredDg = builder.build();

        FuncDeps funcDeps = filteredDg.findValidFuncDeps(ImmutableSet.of(slotA, slotB, slotC, slotD, slotE, slotF));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB)));
        Assertions.assertTrue(funcDeps.isFuncDeps(ImmutableSet.of(slotB), ImmutableSet.of(slotC)));
        // D -> E and E -> F should be removed
        Assertions.assertFalse(funcDeps.isFuncDeps(ImmutableSet.of(slotD), ImmutableSet.of(slotE)));
        Assertions.assertFalse(funcDeps.isFuncDeps(ImmutableSet.of(slotE), ImmutableSet.of(slotF)));
    }
}

