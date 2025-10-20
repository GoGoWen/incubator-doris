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

public class YearsAddVarcharToDateV2Test {

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
        YearsAdd yearsAdd = new YearsAdd(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = yearsAdd.getSignature();

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
        YearsAdd yearsAdd = new YearsAdd(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = yearsAdd.getSignature();

        if (Config.enable_date_conversion) {
            Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT, signature.returnType);
            Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT, signature.argumentsTypes.get(0));
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
        YearsAdd yearsAdd = new YearsAdd(explicitCast, new IntegerLiteral(1));

        FunctionSignature signature = yearsAdd.getSignature();

        // Should use default behavior from ComputeSignatureForDateArithmetic
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
    public void testYearsSubVarcharDirectInputUsesDateV2() {
        // Setup Presto dialect
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Test direct VARCHAR input prefers DATEV2 signature for YearsSub too
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        YearsSub yearsSub = new YearsSub(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = yearsSub.getSignature();

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
    public void testYearsSubExplicitCastFromVarcharPreserved() {
        // Test explicit Cast from VARCHAR is preserved for YearsSub too
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast explicitCast = new Cast(varcharColumn, DateTimeType.INSTANCE, true); // isExplicitType = true
        YearsSub yearsSub = new YearsSub(explicitCast, new IntegerLiteral(1));

        FunctionSignature signature = yearsSub.getSignature();

        // Explicit cast to DATETIME is preserved, returns DATETIME signature
        Assertions.assertEquals(DateTimeType.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateTimeType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testVarcharDirectInputWithDateConversionDisabled() {
        // Set config to false
        Config.enable_date_conversion = false;

        // Setup Presto dialect
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Test direct VARCHAR input with date conversion disabled
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        YearsAdd yearsAdd = new YearsAdd(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = yearsAdd.getSignature();

        // Should use DateType.INSTANCE when Config.enable_date_conversion is false
        Assertions.assertEquals(DateType.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testNonExplicitCastFromVarcharWithDateConversionDisabled() {
        // Set config to false
        Config.enable_date_conversion = false;

        // Setup Presto dialect
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Test non-explicit Cast from VARCHAR with date conversion disabled
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateTimeV2Type.SYSTEM_DEFAULT, false);
        YearsAdd yearsAdd = new YearsAdd(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = yearsAdd.getSignature();

        Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT, signature.returnType);
        Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testYearsSubVarcharDirectInputWithDateConversionDisabled() {
        // Set config to false
        Config.enable_date_conversion = false;

        // Setup Presto dialect
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Test direct VARCHAR input with date conversion disabled for YearsSub
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        YearsSub yearsSub = new YearsSub(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = yearsSub.getSignature();

        // Should use DateType.INSTANCE when Config.enable_date_conversion is false
        Assertions.assertEquals(DateType.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testYearsSubNonExplicitCastFromVarcharWithDateConversionDisabled() {
        // Set config to false
        Config.enable_date_conversion = false;

        // Setup Presto dialect
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Test non-explicit Cast from VARCHAR with date conversion disabled for YearsSub
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateTimeV2Type.SYSTEM_DEFAULT, false);
        YearsSub yearsSub = new YearsSub(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = yearsSub.getSignature();

        Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT, signature.returnType);
        Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testYearsSubDefaultSignatureComputation() {
        // Test YearsSub fallback to default signature computation
        // Use a non-Cast, non-VARCHAR scenario

        // Test with DateTimeV2Type input (not VARCHAR, not Cast)
        SlotReference dateTimeColumn = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT);
        YearsSub yearsSub = new YearsSub(dateTimeColumn, new IntegerLiteral(1));

        FunctionSignature signature = yearsSub.getSignature();

        // Should use default signature computation from ComputeSignatureForDateArithmetic
        Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT, signature.returnType);
        Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testNonExplicitCastToDateV2TypeUsesDateV2SignatureForYearsAdd() {
        // Setup Presto dialect
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Test non-explicit Cast from VARCHAR to DateV2Type
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateV2Type.INSTANCE, false);
        YearsAdd yearsAdd = new YearsAdd(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = yearsAdd.getSignature();

        // Should use DATEV2 signature when cast is to DateV2Type
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testNonExplicitCastToDateTypeUsesDateSignatureForYearsAdd() {
        // Set config to false
        Config.enable_date_conversion = false;

        // Setup Presto dialect
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Test non-explicit Cast from VARCHAR to DateType
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateType.INSTANCE, false);
        YearsAdd yearsAdd = new YearsAdd(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = yearsAdd.getSignature();

        // Should use DateType signature when cast is to DateType and enable_date_conversion is false
        Assertions.assertEquals(DateType.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testNonExplicitCastToDateV2TypeUsesDateV2SignatureForYearsSub() {
        // Setup Presto dialect
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Test non-explicit Cast from VARCHAR to DateV2Type
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateV2Type.INSTANCE, false);
        YearsSub yearsSub = new YearsSub(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = yearsSub.getSignature();

        // Should use DATEV2 signature when cast is to DateV2Type
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testNonExplicitCastToDateTypeUsesDateSignatureForYearsSub() {
        // Set config to false
        Config.enable_date_conversion = false;

        // Setup Presto dialect
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Test non-explicit Cast from VARCHAR to DateType
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateType.INSTANCE, false);
        YearsSub yearsSub = new YearsSub(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = yearsSub.getSignature();

        // Should use DateType signature when cast is to DateType and enable_date_conversion is false
        Assertions.assertEquals(DateType.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testSysdateWithYearsAddUsesDateV2Signature() {
        // Test that years_add(sysdate(-2), -1) uses DATEV2 signature from ComputeSignatureForDateArithmetic
        SysDate sysdate = new SysDate(new IntegerLiteral(-2));
        YearsAdd yearsAdd = new YearsAdd(sysdate, new IntegerLiteral(-1));

        FunctionSignature signature = yearsAdd.getSignature();

        // Should use DATEV2 signature, not DATETIMEV2
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testSysdateWithYearsSubUsesDateV2Signature() {
        // Test that years_sub(sysdate(), 1) uses DATEV2 signature from ComputeSignatureForDateArithmetic
        SysDate sysdate = new SysDate();
        YearsSub yearsSub = new YearsSub(sysdate, new IntegerLiteral(1));

        FunctionSignature signature = yearsSub.getSignature();

        // Should use DATEV2 signature, not DATETIMEV2
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }
}
