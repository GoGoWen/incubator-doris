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

public class MonthsAddVarcharToDateV2Test {

    private boolean originalConfigValue;

    @BeforeEach
    public void setUp() {
        ConnectContext.remove();
        originalConfigValue = Config.enable_date_conversion;
        Config.enable_date_conversion = true;
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        Config.enable_date_conversion = originalConfigValue;
    }

    @Test
    public void testVarcharDirectInputUsesDateV2InPrestoDialect() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        MonthsAdd monthsAdd = new MonthsAdd(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = monthsAdd.getSignature();

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
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateTimeV2Type.SYSTEM_DEFAULT, false);
        MonthsAdd monthsAdd = new MonthsAdd(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = monthsAdd.getSignature();

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
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast explicitCast = new Cast(varcharColumn, DateTimeV2Type.SYSTEM_DEFAULT, true);
        MonthsAdd monthsAdd = new MonthsAdd(explicitCast, new IntegerLiteral(1));

        FunctionSignature signature = monthsAdd.getSignature();

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
    public void testMonthsSubVarcharDirectInputUsesDateV2() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        MonthsSub monthsSub = new MonthsSub(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = monthsSub.getSignature();

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
    public void testMonthsSubExplicitCastFromVarcharPreserved() {
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast explicitCast = new Cast(varcharColumn, DateTimeType.INSTANCE, true);
        MonthsSub monthsSub = new MonthsSub(explicitCast, new IntegerLiteral(1));

        FunctionSignature signature = monthsSub.getSignature();

        Assertions.assertEquals(DateTimeType.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateTimeType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testVarcharDirectInputWithDateConversionDisabled() {
        Config.enable_date_conversion = false;

        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        MonthsAdd monthsAdd = new MonthsAdd(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = monthsAdd.getSignature();

        Assertions.assertEquals(DateType.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testMonthsSubVarcharDirectInputWithDateConversionDisabled() {
        Config.enable_date_conversion = false;

        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        MonthsSub monthsSub = new MonthsSub(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = monthsSub.getSignature();

        Assertions.assertEquals(DateType.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testNonExplicitCastToDateV2TypeUsesDateV2SignatureForMonthsAdd() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateV2Type.INSTANCE, false);
        MonthsAdd monthsAdd = new MonthsAdd(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = monthsAdd.getSignature();

        Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testNonExplicitCastToDateV2TypeUsesDateV2SignatureForMonthsSub() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateV2Type.INSTANCE, false);
        MonthsSub monthsSub = new MonthsSub(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = monthsSub.getSignature();

        Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testSysdateWithMonthsAddUsesDateV2Signature() {
        // Test sysdate() override from ComputeSignatureForDateArithmetic
        SysDate sysdate = new SysDate();
        MonthsAdd monthsAdd = new MonthsAdd(sysdate, new IntegerLiteral(1));

        FunctionSignature signature = monthsAdd.getSignature();

        Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testSysdateWithMonthsSubUsesDateV2Signature() {
        // Test sysdate() override from ComputeSignatureForDateArithmetic
        SysDate sysdate = new SysDate();
        MonthsSub monthsSub = new MonthsSub(sysdate, new IntegerLiteral(1));

        FunctionSignature signature = monthsSub.getSignature();

        Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }
}
