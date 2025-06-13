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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for StringArithmetic functions
 */
public class StringArithmeticTest {

    @Test
    public void testLocate2Args() {
        // Test basic locate function with 2 arguments
        VarcharLiteral substring = new VarcharLiteral("bar");
        VarcharLiteral string = new VarcharLiteral("foobarbar");

        Expression result = StringArithmetic.locate(substring, string);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(4, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3Args() {
        // Test the bug case: locate('bar', 'foobarbar', 5) should return 7
        VarcharLiteral substring = new VarcharLiteral("bar");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(5);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(7, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsNotFound() {
        // Test case where substring is not found after start position
        VarcharLiteral substring = new VarcharLiteral("xyz");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(1);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(0, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsStartPosZero() {
        // Test case where start position is 0 or negative
        VarcharLiteral substring = new VarcharLiteral("bar");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(0);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(0, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsStartPosBeyondString() {
        // Test case where start position is beyond string length
        VarcharLiteral substring = new VarcharLiteral("bar");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(20);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(0, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsFirstOccurrence() {
        // Test case where we start from position 1 and find first occurrence
        VarcharLiteral substring = new VarcharLiteral("bar");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(1);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(4, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsExactMatch() {
        // Test case where substring starts exactly at the start position
        VarcharLiteral substring = new VarcharLiteral("bar");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(4);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(4, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsAfterFirstMatch() {
        // Test case where we start after the first match
        VarcharLiteral substring = new VarcharLiteral("bar");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(6);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(7, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsNegativeStartPos() {
        // Test case where start position is negative
        VarcharLiteral substring = new VarcharLiteral("bar");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(-1);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(0, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsEmptySubstring() {
        // Test case where substring is empty
        VarcharLiteral substring = new VarcharLiteral("");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(1);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(1, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsEmptyString() {
        // Test case where main string is empty
        VarcharLiteral substring = new VarcharLiteral("bar");
        VarcharLiteral string = new VarcharLiteral("");
        IntegerLiteral startPos = new IntegerLiteral(1);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(0, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsBothEmpty() {
        // Test case where both strings are empty
        VarcharLiteral substring = new VarcharLiteral("");
        VarcharLiteral string = new VarcharLiteral("");
        IntegerLiteral startPos = new IntegerLiteral(1);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(0, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsSingleCharacter() {
        // Test case with single character substring
        VarcharLiteral substring = new VarcharLiteral("o");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(3);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(3, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsMultipleOccurrences() {
        // Test case with multiple occurrences of single character
        VarcharLiteral substring = new VarcharLiteral("a");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(6);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(8, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsLastCharacter() {
        // Test case where we search for the last character
        VarcharLiteral substring = new VarcharLiteral("r");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(8);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(9, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsStartAtEnd() {
        // Test case where start position is at the end of string
        VarcharLiteral substring = new VarcharLiteral("r");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(9);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(9, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsStartBeyondEnd() {
        // Test case where start position is beyond the end of string
        VarcharLiteral substring = new VarcharLiteral("r");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(10);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(0, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsLongerSubstring() {
        // Test case where substring is longer than remaining string
        VarcharLiteral substring = new VarcharLiteral("barbarbar");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(5);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(0, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsExactRemainingString() {
        // Test case where substring matches exactly the remaining string
        VarcharLiteral substring = new VarcharLiteral("arbar");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(5);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(5, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate3ArgsCaseSensitive() {
        // Test case to verify case sensitivity
        VarcharLiteral substring = new VarcharLiteral("BAR");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(1);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(0, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate2ArgsNotFound() {
        // Test 2-arg version when substring is not found
        VarcharLiteral substring = new VarcharLiteral("xyz");
        VarcharLiteral string = new VarcharLiteral("foobarbar");

        Expression result = StringArithmetic.locate(substring, string);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(0, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate2ArgsEmptySubstring() {
        // Test 2-arg version with empty substring
        VarcharLiteral substring = new VarcharLiteral("");
        VarcharLiteral string = new VarcharLiteral("foobarbar");

        Expression result = StringArithmetic.locate(substring, string);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(1, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate2ArgsEmptyString() {
        // Test 2-arg version with empty string
        VarcharLiteral substring = new VarcharLiteral("bar");
        VarcharLiteral string = new VarcharLiteral("");

        Expression result = StringArithmetic.locate(substring, string);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(0, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocate2ArgsBothEmpty() {
        // Test 2-arg version with both strings empty
        VarcharLiteral substring = new VarcharLiteral("");
        VarcharLiteral string = new VarcharLiteral("");

        Expression result = StringArithmetic.locate(substring, string);
        Assertions.assertTrue(result instanceof IntegerLiteral);
        Assertions.assertEquals(1, ((IntegerLiteral) result).getValue());
    }

    @Test
    public void testLocateOriginalBugCase() {
        // Test the original bug case mentioned in the issue
        // locate('bar', 'foobarbar', 5) should return 7, not 4
        VarcharLiteral substring = new VarcharLiteral("bar");
        VarcharLiteral string = new VarcharLiteral("foobarbar");
        IntegerLiteral startPos = new IntegerLiteral(5);

        Expression result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertTrue(result instanceof IntegerLiteral);

        // This should return 7 (the position of the second 'bar'), not 4 (the position of the first 'bar')
        Assertions.assertEquals(7, ((IntegerLiteral) result).getValue());

        // Verify that starting from position 1 still finds the first occurrence
        startPos = new IntegerLiteral(1);
        result = StringArithmetic.locate(substring, string, startPos);
        Assertions.assertEquals(4, ((IntegerLiteral) result).getValue());
    }
}
