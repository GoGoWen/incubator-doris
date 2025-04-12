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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DoubleType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AvgTest {
    @Test
    void testSearchSignature() {
        Avg avg = new Avg(new VarcharLiteral("3.1415"));
        FunctionSignature functionSignature = avg.searchSignature(avg.getSignatures());
        Assertions.assertEquals(1, functionSignature.argumentsTypes.size());
        Assertions.assertEquals(DoubleType.INSTANCE, functionSignature.argumentsTypes.get(0));
        Assertions.assertEquals(DoubleType.INSTANCE, functionSignature.returnType);
    }
}
