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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Date;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class PredicateRewriteForPartitionPruneTest extends ExpressionRewriteTestHelper {

    @Test
    public void testVisitInPredicate() {
        SlotReference test = new SlotReference("test", IntegerType.INSTANCE);
        IntegerLiteral i1 = new IntegerLiteral(1);
        IntegerLiteral i2 = new IntegerLiteral(2);
        IntegerLiteral i3 = new IntegerLiteral(3);
        InPredicate inPredicate = new InPredicate(test, Lists.newArrayList(i1, i2, i3));
        Assertions.assertEquals(PredicateRewriteForPartitionPrune.rewrite(inPredicate, context.cascadesContext),
                inPredicate);

        SlotReference testDate = new SlotReference("test_date", DateTimeType.INSTANCE);
        DateLiteral d1 = new DateLiteral("2024-10-14");
        DateLiteral d2 = new DateLiteral("2024-10-15");
        InPredicate inPredicateForDate = new InPredicate(new Date(testDate), Lists.newArrayList(d1, d2));
        List<Expression> splitIn = new ArrayList<>();
        for (Expression opt : inPredicateForDate.getOptions()) {
            GreaterThanEqual ge = new GreaterThanEqual(testDate, ((DateLiteral) opt).toBeginOfTheDay());
            LessThanEqual le = new LessThanEqual(testDate, ((DateLiteral) opt).toEndOfTheDay());
            splitIn.add(new And(ge, le));
        }
        Expression or = ExpressionUtils.combineAsLeftDeepTree(Or.class, splitIn);
        Assertions.assertEquals(or,
                PredicateRewriteForPartitionPrune.rewrite(inPredicateForDate, context.cascadesContext));

        SlotReference testDateV2 = new SlotReference("test_date_v2", DateTimeV2Type.of(6));
        DateLiteral dv1 = new DateV2Literal("2024-10-15");
        DateLiteral dv2 = new DateV2Literal("2024-10-16");
        InPredicate inPredicateForDateV2 = new InPredicate(new Date(testDateV2), Lists.newArrayList(dv1, dv2));
        List<Expression> splitInV2 = new ArrayList<>();
        for (Expression opt : inPredicateForDateV2.getOptions()) {
            GreaterThanEqual ge = new GreaterThanEqual(testDateV2, ((DateV2Literal) opt).toBeginOfTheDay());
            LessThanEqual le = new LessThanEqual(testDateV2, ((DateV2Literal) opt).toEndOfTheDay());
            splitInV2.add(new And(ge, le));
        }

        Expression orV2 = ExpressionUtils.combineAsLeftDeepTree(Or.class, splitInV2);
        Assertions.assertEquals(orV2, PredicateRewriteForPartitionPrune.rewrite(inPredicateForDateV2,
                context.cascadesContext));
    }
}
