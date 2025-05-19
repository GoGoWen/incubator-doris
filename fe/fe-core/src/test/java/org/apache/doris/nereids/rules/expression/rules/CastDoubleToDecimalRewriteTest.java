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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CastDoubleToDecimalRewriteTest extends ExpressionRewriteTestHelper {
    @Test
    public void testRewrite() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();
        Assertions.assertEquals(1, new CastDoubleToDecimalRewrite().buildRules().size());

        Cast cast1 = new Cast(new DoubleLiteral(1.1d), VarcharType.MAX_VARCHAR_TYPE);
        Cast cast2 = new Cast(cast1, DecimalV2Type.SYSTEM_DEFAULT);
        Expression expr1 = CastDoubleToDecimalRewrite.rewrite(cast2);
        Assertions.assertEquals(new Cast(new DoubleLiteral(1.1d), DecimalV2Type.SYSTEM_DEFAULT), expr1);

        Cast cast3 = new Cast(new FloatLiteral(1.1f), VarcharType.MAX_VARCHAR_TYPE);
        Cast cast4 = new Cast(cast3, DecimalV3Type.SYSTEM_DEFAULT);
        Expression expr2 = CastDoubleToDecimalRewrite.rewrite(cast4);
        Assertions.assertEquals(new Cast(new FloatLiteral(1.1f), DecimalV3Type.SYSTEM_DEFAULT), expr2);
    }
}
