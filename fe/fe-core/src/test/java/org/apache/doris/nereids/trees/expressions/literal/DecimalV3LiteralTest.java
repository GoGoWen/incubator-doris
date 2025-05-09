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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.DecimalV3Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

class DecimalV3LiteralTest {
    @Test
    void testConstructorWithBigDecimalOnly() {
        BigDecimal val1 = new BigDecimal("123.45");
        DecimalV3Literal literal1 = new DecimalV3Literal(val1);
        Assertions.assertEquals(0, val1.compareTo(literal1.getValue()));
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(5, 2), literal1.getDataType());

        BigDecimal val2 = new BigDecimal("12345");
        DecimalV3Literal literal2 = new DecimalV3Literal(val2);
        Assertions.assertEquals(0, val2.compareTo(literal2.getValue()));
        // Inferred type for "12345" (scale 0)
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(5, 0), literal2.getDataType());

        BigDecimal val3 = new BigDecimal("0.12345");
        DecimalV3Literal literal3 = new DecimalV3Literal(val3);
        Assertions.assertEquals(0, val3.compareTo(literal3.getValue()));
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(5, 5), literal3.getDataType());
    }

    @Test
    void testConstructorWithExplicitTypeAndValue() {
        DecimalV3Type type = DecimalV3Type.createDecimalV3Type(6, 3);
        BigDecimal value = new BigDecimal("123.456"); // Will be rounded
        DecimalV3Literal literal = new DecimalV3Literal(type, value);

        Assertions.assertEquals(1, new BigDecimal("123.46").compareTo(literal.getValue()));
        Assertions.assertEquals(type, literal.getDataType());
        Assertions.assertEquals(3, literal.getValue().scale()); // Scale should be adjusted

        DecimalV3Type type2 = DecimalV3Type.createDecimalV3Type(10, 3);
        BigDecimal value2 = new BigDecimal("123.45"); // Scale will be adjusted up
        DecimalV3Literal literal2 = new DecimalV3Literal(type2, value2);
        Assertions.assertEquals(0, new BigDecimal("123.450").compareTo(literal2.getValue()));
        Assertions.assertEquals(type2, literal2.getDataType());
        Assertions.assertEquals(3, literal2.getValue().scale());

        // Test with value scale < 0 (e.g. scientific notation that results in integer)
        DecimalV3Type type3 = DecimalV3Type.createDecimalV3Type(10, 2);
        BigDecimal value3 = new BigDecimal("123E2"); // This is 12300, scale is -2
        DecimalV3Literal literal3 = new DecimalV3Literal(type3, value3);
        // Expected value should be 12300.00, constructor adjusts scale to 2
        Assertions.assertEquals(0, new BigDecimal("12300.00").compareTo(literal3.getValue()));
        Assertions.assertEquals(type3, literal3.getDataType());
        Assertions.assertEquals(-2, literal3.getValue().scale());
    }

    @Test
    void testConstructorWithExplicitTypePrecisionScaleCheckFailures() {
        // Case 1: precision < realPrecision
        // value "123.45" (precision=5, scale=2)
        // type (4, 2) -> precision - scale = 2, realPrecision - realScale = 3. 2 < 3, so this should fail.
        // The check is: `precision - scale < realPrecision - realScale`
        // (4-2) < (5-2) => 2 < 3, so this should fail.
        // Also `precision < realPrecision` (4 < 5) is true, so it fails
        DecimalV3Type type1 = DecimalV3Type.createDecimalV3Type(4, 2);
        BigDecimal value1 = new BigDecimal("123.45");
        AnalysisException e1 = Assertions.assertThrows(AnalysisException.class, () -> {
            new DecimalV3Literal(type1, value1);
        });
        Assertions.assertTrue(e1.getMessage().contains("Invalid precision and scale - expect (4, 2), but (5, 2)"));

        // Case 2: scale < realScale (after rounding, this scenario might be tricky to hit before the precision check)
        // The constructor first rounds value.setScale(dataType.getScale(), RoundingMode.HALF_UP);
        // So the realScale check is effectively against the original value.
        DecimalV3Type type2 = DecimalV3Type.createDecimalV3Type(5, 1);
        BigDecimal value2 = new BigDecimal("123.45"); // realPrecision=5, realScale=2
        // type (5,1) -> precision - scale = 4, realPrecision - realScale = 3. 4 is not < 3.
        // precision < realPrecision (5 < 5) is false.
        // scale < realScale (1 < 2) is true. This should fail.
        AnalysisException e2 = Assertions.assertThrows(AnalysisException.class, () -> {
            new DecimalV3Literal(type2, value2);
        });
        Assertions.assertTrue(e2.getMessage().contains("Invalid precision and scale - expect (5, 1), but (5, 2)"));

        // Case 3: precision - scale < realPrecision - realScale (integer part too large)
        // value "1234.5" (realPrecision=5, realScale=1)
        // type (4,1) -> precision - scale = 3, realPrecision - realScale = 4.
        // (4-1) < (5-1) => 3 < 4. This should fail.
        DecimalV3Type type3 = DecimalV3Type.createDecimalV3Type(4, 1);
        BigDecimal value3 = new BigDecimal("1234.5");
        AnalysisException e3 = Assertions.assertThrows(AnalysisException.class, () -> {
            new DecimalV3Literal(type3, value3);
        });
        Assertions.assertTrue(e3.getMessage().contains("Invalid precision and scale - expect (4, 1), but (5, 1)"));

        // Case 4: Null value
        DecimalV3Type type4 = DecimalV3Type.createDecimalV3Type(5, 2);
        Assertions.assertThrows(NullPointerException.class, () -> {
            new DecimalV3Literal(type4, null);
        });
    }

    @Test
    void testGetDouble() {
        DecimalV3Literal literal = new DecimalV3Literal(new BigDecimal("123.4567"));
        Assertions.assertEquals(123.4567, literal.getDouble(), 0.0000001);

        DecimalV3Literal literal2 = new DecimalV3Literal(new BigDecimal("-98.76"));
        Assertions.assertEquals(-98.76, literal2.getDouble(), 0.0000001);
    }

    @Test
    void testRoundCeiling() {
        DecimalV3Literal literal = new DecimalV3Literal(new BigDecimal("123.456")); // scale 3
        Assertions.assertEquals(0, new BigDecimal("123.46").compareTo(literal.roundCeiling(2).getValue()));
        Assertions.assertEquals(0, new BigDecimal("123.5").compareTo(literal.roundCeiling(1).getValue()));
        Assertions.assertEquals(0, new BigDecimal("124").compareTo(literal.roundCeiling(0).getValue()));

        DecimalV3Literal negLiteral = new DecimalV3Literal(new BigDecimal("-123.456"));
        Assertions.assertEquals(0, new BigDecimal("-123.45").compareTo(negLiteral.roundCeiling(2).getValue()));
        Assertions.assertEquals(0, new BigDecimal("-123.4").compareTo(negLiteral.roundCeiling(1).getValue()));
        Assertions.assertEquals(0, new BigDecimal("-123").compareTo(negLiteral.roundCeiling(0).getValue()));
    }

    @Test
    void testRoundFloor() {
        DecimalV3Literal literal = new DecimalV3Literal(new BigDecimal("123.456")); // scale 3
        Assertions.assertEquals(-1, new BigDecimal("123.45").compareTo(literal.roundFloor(2).getValue()));
        Assertions.assertEquals(-1, new BigDecimal("123.4").compareTo(literal.roundFloor(1).getValue()));
        Assertions.assertEquals(-1, new BigDecimal("123").compareTo(literal.roundFloor(0).getValue()));

        DecimalV3Literal negLiteral = new DecimalV3Literal(new BigDecimal("-123.451"));
        Assertions.assertEquals(-1, new BigDecimal("-123.46").compareTo(negLiteral.roundFloor(2).getValue()));
        Assertions.assertEquals(-1, new BigDecimal("-123.5").compareTo(negLiteral.roundFloor(1).getValue()));
        Assertions.assertEquals(-1, new BigDecimal("-124").compareTo(negLiteral.roundFloor(0).getValue()));
    }

    @Test
    void testRoundHalfUp() {
        DecimalV3Literal literal1 = new DecimalV3Literal(new BigDecimal("123.456")); // scale 3
        Assertions.assertEquals(1, new BigDecimal("123.46").compareTo(literal1.round(2).getValue()));

        DecimalV3Literal literal2 = new DecimalV3Literal(new BigDecimal("123.455"));
        Assertions.assertEquals(1, new BigDecimal("123.46").compareTo(literal2.round(2).getValue()));

        DecimalV3Literal literal3 = new DecimalV3Literal(new BigDecimal("123.454"));
        Assertions.assertEquals(0, new BigDecimal("123.45").compareTo(literal3.round(2).getValue()));

        DecimalV3Literal negLiteral = new DecimalV3Literal(new BigDecimal("-123.455"));
        Assertions.assertEquals(0, new BigDecimal("-123.46").compareTo(negLiteral.round(2).getValue()));
    }

    @Test
    void testToLegacyLiteral() {
        BigDecimal val = new BigDecimal("987.65");
        DecimalV3Type type = DecimalV3Type.createDecimalV3Type(val); // P=5, S=2
        DecimalV3Literal literal = new DecimalV3Literal(val);

        org.apache.doris.analysis.LiteralExpr legacy = literal.toLegacyLiteral();
        Assertions.assertTrue(legacy instanceof org.apache.doris.analysis.DecimalLiteral);
        org.apache.doris.analysis.DecimalLiteral legacyDecimal = (org.apache.doris.analysis.DecimalLiteral) legacy;

        Assertions.assertEquals(0, val.compareTo(legacyDecimal.getValue()));
        Assertions.assertEquals(type.toCatalogDataType(), legacyDecimal.getType());
        Assertions.assertEquals(org.apache.doris.catalog.ScalarType.createDecimalV3Type(5, 2), legacyDecimal.getType());
    }

    @Test
    void testEquals() {
        DecimalV3Literal lit1 = new DecimalV3Literal(new BigDecimal("123.45")); // Infers type (5,2)
        DecimalV3Literal lit2 = new DecimalV3Literal(new BigDecimal("123.45")); // Infers type (5,2)
        DecimalV3Literal lit3 = new DecimalV3Literal(new BigDecimal("123.450")); // Infers type (6,3)
        DecimalV3Literal lit4 = new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(5, 2), new BigDecimal("123.45"));
        DecimalV3Literal lit5 = new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(6, 3),
                        new BigDecimal("123.45")); // value becomes 123.450

        Assertions.assertEquals(lit1, lit2); // Same value, same inferred type
        Assertions.assertNotEquals(lit1, lit3); // Same numeric value, different scale -> different inferred type
        Assertions.assertEquals(lit1, lit4); // Same value, same explicit type as lit1's inferred
        Assertions.assertEquals(lit3, lit5); // lit5 internal value 123.450, type (6,3)

        // Test based on super.equals() and specific dataType check
        // FractionalLiteral.equals() compares values using compareTo
        // DecimalV3Literal.equals() then checks dataType
        DecimalV3Literal litA = new DecimalV3Literal(new BigDecimal("1.2")); // Type (2,1), value 1.2
        DecimalV3Literal litB = new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(3, 2),
                    new BigDecimal("1.20")); // Type (2,1), value becomes 1.2

        Assertions.assertEquals(litA.getValue().compareTo(new BigDecimal("1.2")), 0);
        Assertions.assertEquals(litB.getValue().compareTo(new BigDecimal("1.2")), 0);
        Assertions.assertEquals(litA.getDataType(), DecimalV3Type.createDecimalV3Type(2, 1));
        Assertions.assertEquals(litB.getDataType(), DecimalV3Type.createDecimalV3Type(3, 2));

        DecimalV3Literal litC = new DecimalV3Literal(new BigDecimal("1.20")); // Type (3,2), value 1.20
        // litA value is 1.2, litC value is 1.20. BigDecimal.equals is false, but compareTo is 0.
        // The super.equals() in FractionalLiteral likely uses compareTo.
        // So, if super.equals() is true, then it depends on dataType.
        // litA dataType is (2,1), litC dataType is (3,2). So they should not be equal.
        Assertions.assertNotEquals(litA, litC);

        Assertions.assertNotEquals(lit1, null);
        Assertions.assertNotEquals(lit1, new Object());
    }
}
