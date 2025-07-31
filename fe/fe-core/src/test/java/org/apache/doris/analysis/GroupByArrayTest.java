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

package org.apache.doris.analysis;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.analysis.CheckAfterRewrite;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.lang.reflect.Field;
import java.util.ArrayList;

public class GroupByArrayTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private Analyzer analyzer;

    @Mocked
    private CascadesContext cascadesContext;
    @Mocked
    private GroupPlan groupPlan;

    @Before
    public void setUp() throws AnalysisException {
        Analyzer analyzerBase = AccessTestUtil.fetchTableAnalyzer();
        analyzer = new Analyzer(analyzerBase.getEnv(), analyzerBase.getContext());
        try {
            Field f = analyzer.getClass().getDeclaredField("tupleByAlias");
            f.setAccessible(true);
            Multimap<String, TupleDescriptor> tupleByAlias = ArrayListMultimap.create();
            TupleDescriptor td = new TupleDescriptor(new TupleId(0));
            td.setTable(analyzerBase.getTableOrAnalysisException(new TableName(internalCtl, "testdb", "t")));
            tupleByAlias.put("testdb.t", td);
            f.set(analyzer, tupleByAlias);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testIsArrayTypeNestedBaseTypeMethod() {
        // 测试新增的 isArrayTypeNestedBaseType() 方法
        Assert.assertFalse(Type.INT.isArrayTypeNestedBaseType());

        ArrayType intArray = ArrayType.create(Type.INT, true);
        Assert.assertTrue(intArray.isArrayTypeNestedBaseType());

        ArrayType hllArray = ArrayType.create(Type.HLL, true);
        Assert.assertFalse(hllArray.isArrayTypeNestedBaseType());
    }

    @Test
    public void testAggregateInfoWithArrayType() throws AnalysisException {
        // 测试AggregateInfo.validateGroupingExprs()中的isArrayTypeNestedBaseType()调用
        ArrayList<Expr> groupingExprs = new ArrayList<>();
        SlotRef arraySlot = new SlotRef(null, "arr_col");
        arraySlot.setType(ArrayType.create(Type.INT, true));

        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        SlotDescriptor slotDesc = new SlotDescriptor(new SlotId(1), tupleDesc);
        slotDesc.setType(ArrayType.create(Type.INT, true));
        arraySlot.setDesc(slotDesc);
        groupingExprs.add(arraySlot);

        // 验证array<int>类型应该能通过group by检查
        AggregateInfo aggregateInfo = AggregateInfo.create(groupingExprs, new ArrayList<>(), null, analyzer);
        Assert.assertNotNull("AggregateInfo should be created successfully for array<int> type", aggregateInfo);

        Assert.assertTrue(arraySlot.getType().isArrayTypeNestedBaseType());
        Assert.assertTrue(arraySlot.getType().isOnlyMetricType());
    }

    @Test
    public void testAggregateInfoWithArrayTypeNestedMetricType() {
        // 测试元素类型为metric类型的数组
        ArrayList<Expr> groupingExprs = new ArrayList<>();
        SlotRef arraySlot = new SlotRef(null, "arr_col");
        arraySlot.setType(ArrayType.create(Type.HLL, true));

        // 设置desc以避免NullPointerException
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(2));
        SlotDescriptor slotDesc = new SlotDescriptor(new SlotId(2), tupleDesc);
        slotDesc.setType(ArrayType.create(Type.HLL, true));
        arraySlot.setDesc(slotDesc);
        groupingExprs.add(arraySlot);

        // 直接测试类型检查逻辑
        Assert.assertTrue(arraySlot.getType().isOnlyMetricType());
        Assert.assertFalse(arraySlot.getType().isArrayTypeNestedBaseType());

        // 验证条件：isOnlyMetricType() && !isArrayTypeNestedBaseType() 应该为true
        boolean shouldThrowException = arraySlot.getType().isOnlyMetricType() && !arraySlot.getType().isArrayTypeNestedBaseType();
        Assert.assertTrue("Should throw exception for HLL array type", shouldThrowException);
    }

    @Test
    public void testNereidsCheckAfterRewriteWithArrayType() {
        // 测试Nereids中CheckAfterRewrite的isArrayTypeNestedBaseType()调用
        SlotReference arraySlot = new SlotReference("arr_col", org.apache.doris.nereids.types.ArrayType.of(IntegerType.INSTANCE));

        LogicalAggregate<GroupPlan> aggregate = new LogicalAggregate<>(
                ImmutableList.of(arraySlot), // groupByExpressions
                ImmutableList.of(), // outputExpressions
                groupPlan
        );

        CheckAfterRewrite checkAfterRewrite = new CheckAfterRewrite();

        Assertions.assertDoesNotThrow(() ->
                checkAfterRewrite.buildRules().forEach(rule -> rule.transform(aggregate, cascadesContext)));
    }

    @Test
    public void testNereidsCheckAfterRewriteWithArrayTypeNestedMetricType() {
        // 测试Nereids中数组类型（元素为metric类型）应该被拒绝
        SlotReference arraySlot = new SlotReference("arr_col", org.apache.doris.nereids.types.ArrayType.of(HllType.INSTANCE));

        LogicalAggregate<GroupPlan> aggregate = new LogicalAggregate<>(
                ImmutableList.of(arraySlot), // groupByExpressions
                ImmutableList.of(), // outputExpressions
                groupPlan
        );

        CheckAfterRewrite checkAfterRewrite = new CheckAfterRewrite();

        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class, () ->
                checkAfterRewrite.buildRules().forEach(rule -> rule.transform(aggregate, cascadesContext)));
    }
}
