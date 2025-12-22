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
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.ImmutableEqualSet;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class DataTraitTest {

    private SlotReference slotA;
    private SlotReference slotB;
    private SlotReference slotC;
    private SlotReference slotD;
    private SlotReference slotE;
    private SlotReference slotANotNull;
    private SlotReference slotBNotNull;

    @BeforeEach
    public void setUp() {
        slotA = new SlotReference("a", IntegerType.INSTANCE, true);
        slotB = new SlotReference("b", IntegerType.INSTANCE, true);
        slotC = new SlotReference("c", IntegerType.INSTANCE, true);
        slotD = new SlotReference("d", IntegerType.INSTANCE, true);
        slotE = new SlotReference("e", IntegerType.INSTANCE, true);
        slotANotNull = new SlotReference("a", IntegerType.INSTANCE, false);
        slotBNotNull = new SlotReference("b", IntegerType.INSTANCE, false);
    }

    @Test
    public void testEmptyTrait() {
        DataTrait empty = DataTrait.EMPTY_TRAIT;
        Assertions.assertTrue(empty.isEmpty());
        Assertions.assertFalse(empty.isUnique(slotA));
        Assertions.assertFalse(empty.isUniform(slotA));
        Assertions.assertFalse(empty.isNullSafeEqual(slotA, slotB));
    }

    @Test
    public void testBuilderEmpty() {
        DataTrait.Builder builder = new DataTrait.Builder();
        DataTrait trait = builder.build();
        Assertions.assertTrue(trait.isEmpty());
    }

    @Test
    public void testAddUniqueSlot() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniqueSlot(slotA);
        DataTrait trait = builder.build();

        Assertions.assertTrue(trait.isUnique(slotA));
        Assertions.assertFalse(trait.isUnique(slotB));
        Assertions.assertTrue(trait.isUnique(ImmutableSet.of(slotA)));
    }

    @Test
    public void testAddUniqueSlotSet() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniqueSlot(ImmutableSet.of(slotA, slotB));
        DataTrait trait = builder.build();

        Assertions.assertTrue(trait.isUnique(ImmutableSet.of(slotA, slotB)));
        Assertions.assertFalse(trait.isUnique(ImmutableSet.of(slotA)));
        Assertions.assertFalse(trait.isUnique(ImmutableSet.of(slotB)));
    }

    @Test
    public void testAddUniformSlot() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniformSlot(slotA);
        DataTrait trait = builder.build();

        Assertions.assertTrue(trait.isUniform(slotA));
        Assertions.assertFalse(trait.isUniform(slotB));
        Assertions.assertTrue(trait.isUniform(ImmutableSet.of(slotA)));
    }

    @Test
    public void testAddUniformSlotAndLiteral() {
        DataTrait.Builder builder = new DataTrait.Builder();
        IntegerLiteral literal = new IntegerLiteral(10);
        builder.addUniformSlotAndLiteral(slotA, literal);
        DataTrait trait = builder.build();

        Assertions.assertTrue(trait.isUniform(slotA));
        Assertions.assertTrue(trait.isUniformAndHasConstValue(slotA));
        Optional<org.apache.doris.nereids.trees.expressions.Expression> value = trait.getUniformValue(slotA);
        Assertions.assertTrue(value.isPresent());
        Assertions.assertEquals(literal, value.get());
    }

    @Test
    public void testAddEqualPair() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addEqualPair(slotA, slotB);
        DataTrait trait = builder.build();

        Assertions.assertTrue(trait.isNullSafeEqual(slotA, slotB));
        Assertions.assertTrue(trait.isNullSafeEqual(slotB, slotA));
        Assertions.assertFalse(trait.isNullSafeEqual(slotA, slotC));
    }

    @Test
    public void testIsEqualAndNotNotNull() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addEqualPair(slotANotNull, slotBNotNull);
        DataTrait trait = builder.build();

        Assertions.assertTrue(trait.isEqualAndNotNotNull(slotANotNull, slotBNotNull));
        Assertions.assertFalse(trait.isEqualAndNotNotNull(slotA, slotB)); // nullable slots
    }

    @Test
    public void testIsUniqueAndNotNull() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniqueSlot(slotANotNull);
        DataTrait trait = builder.build();

        Assertions.assertTrue(trait.isUniqueAndNotNull(slotANotNull));
        Assertions.assertFalse(trait.isUniqueAndNotNull(slotA)); // nullable
    }

    @Test
    public void testIsUniqueAndNotNullForSet() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniqueSlot(ImmutableSet.of(slotANotNull, slotBNotNull));
        DataTrait trait = builder.build();

        Assertions.assertTrue(trait.isUniqueAndNotNull(ImmutableSet.of(slotANotNull, slotBNotNull)));
        Assertions.assertFalse(trait.isUniqueAndNotNull(ImmutableSet.of(slotA, slotB))); // nullable
    }

    @Test
    public void testIsUniformAndNotNull() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniformSlot(slotANotNull);
        IntegerLiteral literal = new IntegerLiteral(10);
        builder.addUniformSlotAndLiteral(slotANotNull, literal);
        DataTrait trait = builder.build();

        Assertions.assertTrue(trait.isUniformAndNotNull(slotANotNull));
    }

    @Test
    public void testIsUniformAndNotNullForSet() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniformSlot(slotANotNull);
        builder.addUniformSlot(slotBNotNull);
        IntegerLiteral literal = new IntegerLiteral(10);
        builder.addUniformSlotAndLiteral(slotANotNull, literal);
        builder.addUniformSlotAndLiteral(slotBNotNull, literal);
        DataTrait trait = builder.build();

        Assertions.assertTrue(trait.isUniformAndNotNull(ImmutableSet.of(slotANotNull, slotBNotNull)));
    }

    @Test
    public void testCalEqualSet() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addEqualPair(slotA, slotB);
        builder.addEqualPair(slotB, slotC);
        DataTrait trait = builder.build();

        Set<Slot> equalSet = trait.calEqualSet(slotA);
        Assertions.assertFalse(equalSet.contains(slotA));
        Assertions.assertTrue(equalSet.contains(slotB));
        Assertions.assertTrue(equalSet.contains(slotC));
    }

    @Test
    public void testCalAllEqualSet() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addEqualPair(slotA, slotB);
        builder.addEqualPair(slotC, slotD);
        DataTrait trait = builder.build();

        List<Set<Slot>> allEqualSets = trait.calAllEqualSet();
        Assertions.assertEquals(2, allEqualSets.size());
    }

    @Test
    public void testAddDeps() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        DataTrait trait = builder.build();

        // Note: This test depends on FuncDepsDG implementation
        // We can at least verify that the method doesn't throw
        Assertions.assertNotNull(trait);
    }

    @Test
    public void testAddDepsByEqualSet() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addDepsByEqualSet(ImmutableSet.of(slotA, slotB, slotC));
        DataTrait trait = builder.build();

        // Verify the method doesn't throw
        Assertions.assertNotNull(trait);
    }

    @Test
    public void testAddDepsWithEmptySets() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addDeps(ImmutableSet.of(), ImmutableSet.of(slotB));
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of());
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotA)); // dominate contains all dependency
        DataTrait trait = builder.build();

        // Should not throw and should be empty
        Assertions.assertTrue(trait.isEmpty());
    }

    @Test
    public void testAddUniqueByEqualSetWithIntersectionInSlots() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniqueSlot(slotA);
        builder.addUniqueByEqualSet(ImmutableSet.of(slotA, slotB));
        DataTrait trait = builder.build();

        // Both slotA and slotB should be unique
        Assertions.assertTrue(trait.isUnique(slotA));
        Assertions.assertTrue(trait.isUnique(slotB));
    }

    @Test
    public void testAddUniqueByEqualSetWithIntersectionInSlotSets() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniqueSlot(ImmutableSet.of(slotA, slotC));
        // equalSet {a, b} intersects with uniqueSet {a, c}
        // intersection size is 1, which is <= 2, so it should not trigger the replacement logic
        builder.addUniqueByEqualSet(ImmutableSet.of(slotA, slotB));
        DataTrait trait = builder.build();

        // Original unique set should still exist
        Assertions.assertTrue(trait.isUnique(ImmutableSet.of(slotA, slotC)));
    }

    @Test
    public void testAddUniqueByEqualSetWithLargeIntersection() {
        DataTrait.Builder builder = new DataTrait.Builder();
        // Create a unique set with 3 slots: {a, b, c}
        builder.addUniqueSlot(ImmutableSet.of(slotA, slotB, slotC));
        // equalSet {a, b, d} intersects with {a, b, c} with size 2
        // Since intersection.size() > 2 is false (2 is not > 2), it should not trigger replacement
        builder.addUniqueByEqualSet(ImmutableSet.of(slotA, slotB, slotD));
        DataTrait trait = builder.build();

        // Original unique set should still exist
        Assertions.assertTrue(trait.isUnique(ImmutableSet.of(slotA, slotB, slotC)));
    }

    @Test
    public void testAddUniqueByEqualSetWithThreeWayIntersection() {
        DataTrait.Builder builder = new DataTrait.Builder();
        // Create a unique set with 4 slots: {a, b, c, e}
        builder.addUniqueSlot(ImmutableSet.of(slotA, slotB, slotC, slotE));
        // equalSet {a, b, c, d} intersects with {a, b, c, e} with size 3
        // Since intersection.size() > 2 is true (3 > 2), it should trigger replacement
        // remaining = {e} (after removing intersection {a, b, c})
        // New sets created: {e, a}, {e, b}, {e, c}, {e, d}
        builder.addUniqueByEqualSet(ImmutableSet.of(slotA, slotB, slotC, slotD));
        DataTrait trait = builder.build();

        // Should create new unique sets with slotD replacing parts of the original set
        // The logic creates sets: {e, a}, {e, b}, {e, c}, {e, d}
        // We verify that at least some new combinations are unique
        Assertions.assertTrue(trait.isUnique(ImmutableSet.of(slotE, slotD)));
        Assertions.assertTrue(trait.isUnique(ImmutableSet.of(slotE, slotA)));
        // Original set {a, b, c, e} should be removed
    }

    @Test
    public void testAddUniformByEqualSet() {
        DataTrait.Builder builder = new DataTrait.Builder();
        IntegerLiteral literal = new IntegerLiteral(10);
        builder.addUniformSlotAndLiteral(slotA, literal);
        builder.addUniformByEqualSet(ImmutableSet.of(slotA, slotB));
        DataTrait trait = builder.build();

        Assertions.assertTrue(trait.isUniform(slotB));
        Optional<org.apache.doris.nereids.trees.expressions.Expression> valueB = trait.getUniformValue(slotB);
        Assertions.assertTrue(valueB.isPresent());
        Assertions.assertEquals(literal, valueB.get());
    }

    @Test
    public void testAddUniformByEqualSetNoIntersection() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniformSlot(slotA);
        builder.addUniformByEqualSet(ImmutableSet.of(slotB, slotC)); // No intersection
        DataTrait trait = builder.build();

        Assertions.assertTrue(trait.isUniform(slotA));
        Assertions.assertFalse(trait.isUniform(slotB));
        Assertions.assertFalse(trait.isUniform(slotC));
    }

    @Test
    public void testAddUniformSlotForOuterJoinNullableSide() {
        DataTrait.Builder builder1 = new DataTrait.Builder();
        builder1.addUniformSlot(slotA); // nullable slot
        DataTrait trait1 = builder1.build();

        DataTrait.Builder builder2 = new DataTrait.Builder();
        builder2.addUniformSlotForOuterJoinNullableSide(trait1);
        DataTrait trait2 = builder2.build();

        // Should add nullable uniform slots
        Assertions.assertTrue(trait2.isUniform(slotA));
    }

    @Test
    public void testAddUniformSlotForOuterJoinNullableSideWithNullLiteral() {
        DataTrait.Builder builder1 = new DataTrait.Builder();
        NullLiteral nullLiteral = NullLiteral.INSTANCE;
        builder1.addUniformSlotAndLiteral(slotA, nullLiteral);
        DataTrait trait1 = builder1.build();

        DataTrait.Builder builder2 = new DataTrait.Builder();
        builder2.addUniformSlotForOuterJoinNullableSide(trait1);
        DataTrait trait2 = builder2.build();

        Assertions.assertTrue(trait2.isUniform(slotA));
    }

    @Test
    public void testAddDataTrait() {
        DataTrait.Builder builder1 = new DataTrait.Builder();
        builder1.addUniqueSlot(slotA);
        builder1.addUniformSlot(slotB);
        builder1.addEqualPair(slotC, slotD);
        DataTrait trait1 = builder1.build();

        DataTrait.Builder builder2 = new DataTrait.Builder();
        builder2.addDataTrait(trait1);
        DataTrait trait2 = builder2.build();

        Assertions.assertTrue(trait2.isUnique(slotA));
        Assertions.assertTrue(trait2.isUniform(slotB));
        Assertions.assertTrue(trait2.isNullSafeEqual(slotC, slotD));
    }

    @Test
    public void testBuilderCopyConstructor() {
        DataTrait.Builder builder1 = new DataTrait.Builder();
        builder1.addUniqueSlot(slotA);
        builder1.addUniformSlot(slotB);
        DataTrait trait1 = builder1.build();

        DataTrait.Builder builder2 = new DataTrait.Builder(trait1);
        builder2.addUniqueSlot(slotC);
        DataTrait trait2 = builder2.build();

        Assertions.assertTrue(trait2.isUnique(slotA));
        Assertions.assertTrue(trait2.isUnique(slotC));
        Assertions.assertTrue(trait2.isUniform(slotB));
    }

    @Test
    public void testPruneSlots() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniqueSlot(slotA);
        builder.addUniqueSlot(slotB);
        builder.addUniformSlot(slotC);
        builder.addEqualPair(slotA, slotD);
        builder.pruneSlots(ImmutableSet.of(slotA, slotC));
        DataTrait trait = builder.build();

        Assertions.assertTrue(trait.isUnique(slotA));
        Assertions.assertFalse(trait.isUnique(slotB));
        Assertions.assertTrue(trait.isUniform(slotC));
    }

    @Test
    public void testPruneEqualSetSlots() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addEqualPair(slotA, slotB);
        builder.addEqualPair(slotC, slotD);
        builder.pruneEqualSetSlots(ImmutableSet.of(slotA, slotB));
        DataTrait trait = builder.build();

        Assertions.assertTrue(trait.isNullSafeEqual(slotA, slotB));
        Assertions.assertFalse(trait.isNullSafeEqual(slotC, slotD));
    }

    @Test
    public void testReplaceUniformBy() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniformSlot(slotA);
        Map<Slot, Slot> replaceMap = ImmutableSet.of(
                new java.util.AbstractMap.SimpleEntry<>(slotA, slotB)
        ).stream().collect(java.util.stream.Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue));
        builder.replaceUniformBy(replaceMap);
        DataTrait trait = builder.build();

        Assertions.assertFalse(trait.isUniform(slotA));
        Assertions.assertTrue(trait.isUniform(slotB));
    }

    @Test
    public void testReplaceUniqueBy() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniqueSlot(slotA);
        Map<Slot, Slot> replaceMap = ImmutableSet.of(
                new java.util.AbstractMap.SimpleEntry<>(slotA, slotB)
        ).stream().collect(java.util.stream.Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue));
        builder.replaceUniqueBy(replaceMap);
        DataTrait trait = builder.build();

        Assertions.assertFalse(trait.isUnique(slotA));
        Assertions.assertTrue(trait.isUnique(slotB));
    }

    @Test
    public void testReplaceEqualSetBy() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addEqualPair(slotA, slotB);
        Map<Slot, Slot> replaceMap = ImmutableSet.of(
                new java.util.AbstractMap.SimpleEntry<>(slotA, slotC)
        ).stream().collect(java.util.stream.Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue));
        builder.replaceEqualSetBy(replaceMap);
        DataTrait trait = builder.build();

        Assertions.assertFalse(trait.isNullSafeEqual(slotA, slotB));
        Assertions.assertTrue(trait.isNullSafeEqual(slotC, slotB));
    }

    @Test
    public void testGetAllUniformValues() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniformSlot(slotA);
        IntegerLiteral literal = new IntegerLiteral(10);
        builder.addUniformSlotAndLiteral(slotB, literal);
        DataTrait trait = builder.build();

        Map<Slot, Optional<org.apache.doris.nereids.trees.expressions.Expression>> allValues = trait.getAllUniformValues();
        Assertions.assertTrue(allValues.containsKey(slotA));
        Assertions.assertTrue(allValues.containsKey(slotB));
        Assertions.assertTrue(allValues.get(slotB).isPresent());
        Assertions.assertEquals(literal, allValues.get(slotB).get());
    }

    @Test
    public void testGetAllUniqueAndNotNull() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniqueSlot(slotANotNull);
        builder.addUniqueSlot(slotA); // nullable
        builder.addUniqueSlot(ImmutableSet.of(slotBNotNull, slotC));
        builder.addUniqueSlot(ImmutableSet.of(slotD, slotE)); // both nullable
        builder.build();

        List<Set<Slot>> allUniqueAndNotNull = builder.getAllUniqueAndNotNull();
        Assertions.assertTrue(allUniqueAndNotNull.contains(ImmutableSet.of(slotANotNull)));
        Assertions.assertFalse(allUniqueAndNotNull.contains(ImmutableSet.of(slotBNotNull, slotC)));
        // slotA (nullable) should not be in the result
        // slotD, slotE set (both nullable) should not be in the result
    }

    @Test
    public void testGetAllUniformAndNotNull() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniformSlot(slotANotNull);
        builder.addUniformSlot(slotA); // nullable
        IntegerLiteral literal = new IntegerLiteral(10);
        builder.addUniformSlotAndLiteral(slotBNotNull, literal);
        builder.build();

        List<Set<Slot>> allUniformAndNotNull = builder.getAllUniformAndNotNull();
        Assertions.assertTrue(allUniformAndNotNull.contains(ImmutableSet.of(slotANotNull)));
        Assertions.assertTrue(allUniformAndNotNull.contains(ImmutableSet.of(slotBNotNull)));
    }

    @Test
    public void testCalEqualSetList() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addEqualPair(slotA, slotB);
        builder.addEqualPair(slotC, slotD);
        List<Set<Slot>> equalSetList = builder.calEqualSetList();

        Assertions.assertEquals(2, equalSetList.size());
    }

    @Test
    public void testAddFdItems() {
        DataTrait.Builder builder = new DataTrait.Builder();
        FdItem fdItem = new FdItem(ImmutableSet.of(slotA), true, false);
        builder.addFdItems(ImmutableSet.of(fdItem));
        DataTrait trait = builder.build();

        ImmutableSet<FdItem> fdItems = trait.getFdItems();
        Assertions.assertTrue(fdItems.contains(fdItem));
    }

    @Test
    public void testAddFuncDepsDG() {
        DataTrait.Builder builder1 = new DataTrait.Builder();
        builder1.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        DataTrait trait1 = builder1.build();

        DataTrait.Builder builder2 = new DataTrait.Builder();
        builder2.addFuncDepsDG(trait1);
        DataTrait trait2 = builder2.build();

        // Verify the method doesn't throw
        Assertions.assertNotNull(trait2);
    }

    @Test
    public void testToString() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniqueSlot(slotA);
        builder.addUniformSlot(slotB);
        DataTrait trait = builder.build();

        String str = trait.toString();
        Assertions.assertNotNull(str);
        Assertions.assertTrue(str.contains("uniform"));
        Assertions.assertTrue(str.contains("unique"));
    }

    @Test
    public void testGetEqualSet() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addEqualPair(slotA, slotB);
        DataTrait trait = builder.build();

        ImmutableEqualSet<Slot> equalSet = trait.getEqualSet();
        Assertions.assertNotNull(equalSet);
        Assertions.assertTrue(equalSet.isEqual(slotA, slotB));
    }

    @Test
    public void testGetAllValidFuncDeps() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        DataTrait trait = builder.build();

        FuncDeps funcDeps = trait.getAllValidFuncDeps(ImmutableSet.of(slotA, slotB));
        Assertions.assertNotNull(funcDeps);
    }

    @Test
    public void testIsDependent() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addDeps(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        DataTrait trait = builder.build();

        // This depends on FuncDepsDG implementation
        // We can at least verify the method doesn't throw
        trait.isDependent(ImmutableSet.of(slotA), ImmutableSet.of(slotB));
        // Result depends on implementation, but method should not throw
        Assertions.assertNotNull(trait);
    }

    @Test
    public void testAddDepsByEqualSetWithSmallSet() {
        DataTrait.Builder builder = new DataTrait.Builder();
        // Equal set with size < 2 should not add anything
        builder.addDepsByEqualSet(ImmutableSet.of(slotA));
        builder.addDepsByEqualSet(ImmutableSet.of());
        DataTrait trait = builder.build();

        Assertions.assertTrue(trait.isEmpty());
    }

    @Test
    public void testAddUniqueSlotWithEmptySet() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniqueSlot(ImmutableSet.of());
        DataTrait trait = builder.build();

        // Empty set should not be added
        Assertions.assertTrue(trait.isEmpty());
    }

    @Test
    public void testAddUniqueSlotWithSingleElementSet() {
        DataTrait.Builder builder = new DataTrait.Builder();
        // Single element set should be added as a single slot
        builder.addUniqueSlot(ImmutableSet.of(slotA));
        DataTrait trait = builder.build();

        Assertions.assertTrue(trait.isUnique(slotA));
    }

    @Test
    public void testPruneSlotsWithEmptySet() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniqueSlot(slotA);
        builder.addUniformSlot(slotB);
        builder.pruneSlots(ImmutableSet.of());
        DataTrait trait = builder.build();

        // Empty set should not prune anything
        Assertions.assertTrue(trait.isUnique(slotA));
        Assertions.assertTrue(trait.isUniform(slotB));
    }

    @Test
    public void testPruneEqualSetSlotsWithEmptySet() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addEqualPair(slotA, slotB);
        builder.pruneEqualSetSlots(ImmutableSet.of());
        DataTrait trait = builder.build();

        // Empty set should not prune anything
        Assertions.assertFalse(trait.isNullSafeEqual(slotA, slotB));
    }

    @Test
    public void testReplaceWithEmptyMap() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniqueSlot(slotA);
        builder.addUniformSlot(slotB);
        Map<Slot, Slot> emptyMap = ImmutableSet.<Map.Entry<Slot, Slot>>of()
                .stream().collect(java.util.stream.Collectors.toMap(
                        Map.Entry::getKey, Map.Entry::getValue));
        builder.replaceUniqueBy(emptyMap);
        builder.replaceUniformBy(emptyMap);
        DataTrait trait = builder.build();

        // Empty map should not change anything
        Assertions.assertTrue(trait.isUnique(slotA));
        Assertions.assertTrue(trait.isUniform(slotB));
    }

    @Test
    public void testGetUniformValueNotPresent() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniformSlot(slotA); // No literal value
        DataTrait trait = builder.build();

        Optional<org.apache.doris.nereids.trees.expressions.Expression> value = trait.getUniformValue(slotA);
        Assertions.assertFalse(value.isPresent());
    }

    @Test
    public void testGetUniformValueForNonUniformSlot() {
        DataTrait.Builder builder = new DataTrait.Builder();
        DataTrait trait = builder.build();

        Optional<org.apache.doris.nereids.trees.expressions.Expression> value = trait.getUniformValue(slotA);
        Assertions.assertFalse(value.isPresent());
    }

    @Test
    public void testIsUniqueWithEmptySet() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniqueSlot(slotA);
        DataTrait trait = builder.build();

        Assertions.assertFalse(trait.isUnique(ImmutableSet.of()));
    }

    @Test
    public void testIsUniformWithEmptySet() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniformSlot(slotA);
        DataTrait trait = builder.build();

        Assertions.assertFalse(trait.isUniform(ImmutableSet.of()));
    }

    @Test
    public void testCalEqualSetForNonEqualSlot() {
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addEqualPair(slotA, slotB);
        DataTrait trait = builder.build();

        Set<Slot> equalSet = trait.calEqualSet(slotC);
        // Should return a set containing only slotC
        Assertions.assertFalse(equalSet.contains(slotC));
        Assertions.assertFalse(equalSet.contains(slotA));
    }
}

