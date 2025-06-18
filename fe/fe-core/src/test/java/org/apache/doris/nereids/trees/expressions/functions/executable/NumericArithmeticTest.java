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

package org.apache.doris.nereids.trees.expressions.functions.executable;

import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for NumericArithmetic functions
 */
public class NumericArithmeticTest {

    @Test
    public void testLog() {
        // Test normal case
        Expression result = NumericArithmetic.log(new DoubleLiteral(2.0), new DoubleLiteral(8.0));
        Assertions.assertEquals(3.0, ((DoubleLiteral) result).getValue(), 0.0001);

        // Test base = 1 (should throw exception)
        Assertions.assertThrows(NotSupportedException.class, () -> {
            NumericArithmetic.log(new DoubleLiteral(1.0), new DoubleLiteral(8.0));
        });

        // Test negative number (should throw exception)
        Assertions.assertThrows(NotSupportedException.class, () -> {
            NumericArithmetic.log(new DoubleLiteral(2.0), new DoubleLiteral(-8.0));
        });

        // Test zero (should throw exception)
        Assertions.assertThrows(NotSupportedException.class, () -> {
            NumericArithmetic.log(new DoubleLiteral(2.0), new DoubleLiteral(0.0));
        });
    }

    @Test
    public void testFmodDouble() {
        // Test normal case
        Expression result = NumericArithmetic.fmod(new DoubleLiteral(10.0), new DoubleLiteral(3.0));
        Assertions.assertEquals(1.0, ((DoubleLiteral) result).getValue(), 0.0001);

        // Test negative numbers
        result = NumericArithmetic.fmod(new DoubleLiteral(-10.0), new DoubleLiteral(3.0));
        Assertions.assertEquals(-1.0, ((DoubleLiteral) result).getValue(), 0.0001);
    }

    @Test
    public void testFmodFloat() {
        // Test normal case
        Expression result = NumericArithmetic.fmod(new FloatLiteral(10.0f), new FloatLiteral(3.0f));
        Assertions.assertEquals(1.0f, ((FloatLiteral) result).getValue(), 0.0001);

        // Test negative numbers
        result = NumericArithmetic.fmod(new FloatLiteral(-10.0f), new FloatLiteral(3.0f));
        Assertions.assertEquals(-1.0f, ((FloatLiteral) result).getValue(), 0.0001);
    }
}
