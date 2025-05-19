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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Concat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Floor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Rewrite rule of simplify CAST expression.
 *
 * --cast(cast(2493.0 as double) as varchar)
 */
public class ConvertPrestoDoubleToString implements ExpressionPatternRuleFactory {

    public static final ConvertPrestoDoubleToString INSTANCE = new ConvertPrestoDoubleToString();

    /**
     * subclass of Cast
     *
     */
    public static class DoubleToVarcharCast extends Cast {

        /**
         * constructor method
         */
        public DoubleToVarcharCast(Expression child, DataType targetType) {
            super(child, targetType);
        }

        @Override
        public Cast withChildren(List<Expression> children) {
            Preconditions.checkArgument(children.size() == 1);
            return new DoubleToVarcharCast(children.get(0), VarcharType.SYSTEM_DEFAULT);
        }
    }

    private boolean isCastToDecimal(Expression expression) {
        if (expression instanceof Cast) {
            Cast cast = (Cast) expression;
            return cast.getDataType() instanceof DecimalV3Type || cast.getDataType() instanceof DecimalV2Type;
        }
        return false;
    }

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Cast.class)
                        .whenCtx(ctx -> ctx.cascadesContext.getConnectContext().getSessionVariable()
                                .getSqlDialect().equalsIgnoreCase("presto")
                                && (ctx.expr.child(0).getDataType() instanceof DoubleType
                                    || ctx.expr.child(0).getDataType() instanceof FloatType)
                                && ctx.expr.getDataType().isStringLikeType()
                                && !(ctx.expr instanceof DoubleToVarcharCast)
                                && (!ctx.parent.isPresent() || !isCastToDecimal(ctx.parent.get())))
                        .then(cast -> {
                            Expression doubleExpr = cast.child();
                            Cast castDoubleAsVarchar = new DoubleToVarcharCast(doubleExpr, cast.getDataType());
                            if (doubleExpr.getDataType() instanceof FloatType) {
                                doubleExpr = new Cast(doubleExpr, DoubleType.INSTANCE);
                            }
                            return new If(
                                    new EqualTo(cast.child(), new Floor(doubleExpr)),
                                    new Concat(castDoubleAsVarchar, new VarcharLiteral(".0")),
                                    castDoubleAsVarchar
                            );
                        })
        );
    }
}
