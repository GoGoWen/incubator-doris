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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DaysAddVarcharToDateV2Test {

    private boolean originalConfigValue;

    @BeforeEach
    public void setUp() {
        ConnectContext.remove();
        // Save original config value
        originalConfigValue = Config.enable_date_conversion;
        // Ensure default value is true for most tests
        Config.enable_date_conversion = true;
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        // Restore original config value
        Config.enable_date_conversion = originalConfigValue;
    }

    @Test
    public void testVarcharDirectInputUsesDateV2InPrestoDialect() {
        // Setup Presto dialect
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Test direct VARCHAR input prefers DATEV2 signature in Presto dialect
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        DaysAdd daysAdd = new DaysAdd(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = daysAdd.getSignature();

        // Should use DATEV2 signature when Config.enable_date_conversion is true
        if (Config.enable_date_conversion) {
            Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
            Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        } else {
            Assertions.assertEquals(DateType.INSTANCE, signature.returnType);
            Assertions.assertEquals(DateType.INSTANCE, signature.argumentsTypes.get(0));
        }
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testNonExplicitCastFromVarcharUsesDateV2InPrestoDialect() {
        // Setup Presto dialect
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Test non-explicit Cast from VARCHAR prefers DATEV2 signature in Presto dialect
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateTimeV2Type.SYSTEM_DEFAULT, false); // isExplicitType = false
        DaysAdd daysAdd = new DaysAdd(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = daysAdd.getSignature();

        // Should use DATEV2 signature even when cast is to DATETIMEV2
        if (Config.enable_date_conversion) {
            Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
            Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        } else {
            Assertions.assertEquals(DateType.INSTANCE, signature.returnType);
            Assertions.assertEquals(DateType.INSTANCE, signature.argumentsTypes.get(0));
        }
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testExplicitCastFromVarcharPreservedInPrestoDialect() {
        // Setup Presto dialect
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Test explicit Cast from VARCHAR is preserved (no override) even in Presto dialect
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast explicitCast = new Cast(varcharColumn, DateTimeV2Type.SYSTEM_DEFAULT, true); // isExplicitType = true
        DaysAdd daysAdd = new DaysAdd(explicitCast, new IntegerLiteral(1));

        FunctionSignature signature = daysAdd.getSignature();

        // Should use default behavior from ComputeSignatureForDateArithmetic
        // This will return the default signature based on Config.enable_date_conversion
        if (Config.enable_date_conversion) {
            Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT, signature.returnType);
            Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT, signature.argumentsTypes.get(0));
        } else {
            Assertions.assertEquals(DateTimeType.INSTANCE, signature.returnType);
            Assertions.assertEquals(DateTimeType.INSTANCE, signature.argumentsTypes.get(0));
        }
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testDaysSubVarcharDirectInputUsesDateV2() {
        // Setup Presto dialect
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Test direct VARCHAR input prefers DATEV2 signature for DaysSub too
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        DaysSub daysSub = new DaysSub(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = daysSub.getSignature();

        // Should use DATEV2 signature when Config.enable_date_conversion is true
        if (Config.enable_date_conversion) {
            Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
            Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        } else {
            Assertions.assertEquals(DateType.INSTANCE, signature.returnType);
            Assertions.assertEquals(DateType.INSTANCE, signature.argumentsTypes.get(0));
        }
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testDaysSubExplicitCastFromVarcharPreserved() {
        // Test explicit Cast from VARCHAR is preserved for DaysSub too
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast explicitCast = new Cast(varcharColumn, DateTimeType.INSTANCE, true); // isExplicitType = true
        DaysSub daysSub = new DaysSub(explicitCast, new IntegerLiteral(1));

        FunctionSignature signature = daysSub.getSignature();

        // Explicit cast to DATETIME is preserved, returns DATETIME signature
        Assertions.assertEquals(DateTimeType.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateTimeType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testVarcharDirectInputWithDateConversionDisabled() {
        // Set config to false to test the else branch (lines 91-92 in DaysAdd)
        Config.enable_date_conversion = false;

        // Setup Presto dialect
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Test direct VARCHAR input with date conversion disabled
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        DaysAdd daysAdd = new DaysAdd(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = daysAdd.getSignature();

        // Should use DateType.INSTANCE when Config.enable_date_conversion is false
        Assertions.assertEquals(DateType.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testNonExplicitCastFromVarcharWithDateConversionDisabled() {
        // Set config to false to test the else branch (lines 106-107 in DaysAdd)
        Config.enable_date_conversion = false;

        // Setup Presto dialect
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Test non-explicit Cast from VARCHAR with date conversion disabled
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateTimeV2Type.SYSTEM_DEFAULT, false); // isExplicitType = false
        DaysAdd daysAdd = new DaysAdd(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = daysAdd.getSignature();

        // Should use DateType.INSTANCE when Config.enable_date_conversion is false
        Assertions.assertEquals(DateType.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testDaysSubVarcharDirectInputWithDateConversionDisabled() {
        // Set config to false to test the else branch (lines 91-92 in DaysSub)
        Config.enable_date_conversion = false;

        // Setup Presto dialect
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Test direct VARCHAR input with date conversion disabled for DaysSub
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        DaysSub daysSub = new DaysSub(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = daysSub.getSignature();

        // Should use DateType.INSTANCE when Config.enable_date_conversion is false
        Assertions.assertEquals(DateType.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testDaysSubNonExplicitCastFromVarcharWithDateConversionDisabled() {
        // Set config to false to test the else branches (lines 96-100, 102-104, 106-107 in DaysSub)
        Config.enable_date_conversion = false;

        // Setup Presto dialect
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Test non-explicit Cast from VARCHAR with date conversion disabled for DaysSub
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateTimeV2Type.SYSTEM_DEFAULT, false); // isExplicitType = false
        DaysSub daysSub = new DaysSub(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = daysSub.getSignature();

        // Should use DateType.INSTANCE when Config.enable_date_conversion is false
        Assertions.assertEquals(DateType.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testDaysSubDefaultSignatureComputation() {
        // Test DaysSub fallback to default signature computation (line 112 in DaysSub)
        // Use a non-Cast, non-VARCHAR scenario to trigger the super.computeSignature() call

        // Test with DateTimeV2Type input (not VARCHAR, not Cast)
        SlotReference dateTimeColumn = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT);
        DaysSub daysSub = new DaysSub(dateTimeColumn, new IntegerLiteral(1));

        FunctionSignature signature = daysSub.getSignature();

        // Should use default signature computation from ComputeSignatureForDateArithmetic
        // This tests the super.computeSignature() fallback path
        Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT, signature.returnType);
        Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }
}
