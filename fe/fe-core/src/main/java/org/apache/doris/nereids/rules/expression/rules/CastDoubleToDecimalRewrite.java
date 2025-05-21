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

import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Trim;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * rewrite cast double to decimal in presto sql dialect
 */
public class CastDoubleToDecimalRewrite implements ExpressionPatternRuleFactory {

    public static final CastDoubleToDecimalRewrite INSTANCE = new CastDoubleToDecimalRewrite();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Cast.class).then(CastDoubleToDecimalRewrite::rewrite),
                matchesType(Trim.class).then(CastDoubleToDecimalRewrite::rewrite)
        );
    }

    /** rewrite cast*/
    public static Expression rewrite(Cast cast) {
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().getSqlDialect().equalsIgnoreCase(
                "presto")) {
            Expression expr = cast.child();
            if (expr.getDataType() instanceof CharacterType && (cast.getDataType() instanceof DecimalV3Type
                    || cast.getDataType() instanceof DecimalV2Type)) {
                if (expr instanceof Cast && (((Cast) expr).child().getDataType() instanceof FloatType
                        || ((Cast) expr).child().getDataType() instanceof DoubleType)) {
                    return new Cast(expr.child(0), cast.getDataType(), cast.isExplicitType());
                }
            }
        }
        return cast;
    }

    /** rewrite trim*/
    public static Expression rewrite(Trim trim) {
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().getSqlDialect().equalsIgnoreCase(
                "presto")) {
            Expression expr = trim.child();
            if (expr instanceof Cast && (expr.child(0).getDataType() instanceof DoubleType
                    || expr.child(0).getDataType() instanceof FloatType)) {
                return expr;
            }
        }
        return trim;
    }
}
