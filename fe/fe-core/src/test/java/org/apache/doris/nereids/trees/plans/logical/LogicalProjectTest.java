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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.apache.hive.common.util.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class LogicalProjectTest {

    private final Plan mockChild = Mockito.mock(Plan.class);

    @Test
    public void testWithNoPermissionColumn() {
        Column mockColumn = Mockito.mock(Column.class);
        Mockito.when(mockColumn.getComment()).thenReturn(Constants.JD_SHIELDING_COLUMN);

        SlotReference slot = new SlotReference(
                null, "col1", IntegerType.INSTANCE, false,
                ImmutableList.of(), null, mockColumn
        );
        NamedExpression parent = new Alias(new IntegerLiteral(1), "parent");
        LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(parent), mockChild);
        Assertions.assertFalse(project.hasColumnPermission(slot));
    }

    @Test
    public void testWithNormalColumn() {
        Column mockColumn = Mockito.mock(Column.class);
        Mockito.when(mockColumn.getComment()).thenReturn("normal column comment");

        SlotReference slot = new SlotReference(
                null, "col1", IntegerType.INSTANCE, false,
                ImmutableList.of(), null, mockColumn
        );

        NamedExpression parent = new Alias(new IntegerLiteral(1), "parent");
        LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(parent), mockChild);
        Assertions.assertTrue(project.hasColumnPermission(slot));
    }

    @Test
    public void testWithoutColumn() {
        SlotReference slot = new SlotReference(
                null, "col1", IntegerType.INSTANCE, false,
                ImmutableList.of(), null, (Column) null
        );

        NamedExpression parent = new Alias(new IntegerLiteral(1), "parent");
        LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(parent), mockChild);
        Assertions.assertTrue(project.hasColumnPermission(slot));
    }

    @Test
    public void testNonSlotExpression() {
        NamedExpression parent = new Alias(new IntegerLiteral(1), "parent");
        NamedExpression alias = new Alias(new IntegerLiteral(1), "clo1");
        LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(parent), mockChild);
        Assertions.assertTrue(project.hasColumnPermission(alias));
    }
}
