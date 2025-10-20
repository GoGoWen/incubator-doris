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

public class WeeksAddVarcharToDateV2Test {

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
        WeeksAdd weeksAdd = new WeeksAdd(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = weeksAdd.getSignature();

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
        WeeksAdd weeksAdd = new WeeksAdd(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = weeksAdd.getSignature();

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
        WeeksAdd weeksAdd = new WeeksAdd(explicitCast, new IntegerLiteral(1));

        FunctionSignature signature = weeksAdd.getSignature();

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
    public void testWeeksSubVarcharDirectInputUsesDateV2() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        WeeksSub weeksSub = new WeeksSub(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = weeksSub.getSignature();

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
    public void testWeeksSubExplicitCastFromVarcharPreserved() {
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast explicitCast = new Cast(varcharColumn, DateTimeType.INSTANCE, true);
        WeeksSub weeksSub = new WeeksSub(explicitCast, new IntegerLiteral(1));

        FunctionSignature signature = weeksSub.getSignature();

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
        WeeksAdd weeksAdd = new WeeksAdd(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = weeksAdd.getSignature();

        Assertions.assertEquals(DateType.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testWeeksSubVarcharDirectInputWithDateConversionDisabled() {
        Config.enable_date_conversion = false;

        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        WeeksSub weeksSub = new WeeksSub(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = weeksSub.getSignature();

        Assertions.assertEquals(DateType.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testWeeksSubDefaultSignatureComputation() {
        SlotReference dateTimeColumn = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT);
        WeeksSub weeksSub = new WeeksSub(dateTimeColumn, new IntegerLiteral(1));

        FunctionSignature signature = weeksSub.getSignature();

        Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT, signature.returnType);
        Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testNonExplicitCastToDateV2TypeUsesDateV2SignatureForWeeksAdd() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateV2Type.INSTANCE, false);
        WeeksAdd weeksAdd = new WeeksAdd(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = weeksAdd.getSignature();

        Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testNonExplicitCastToDateTypeUsesDateSignatureForWeeksAdd() {
        Config.enable_date_conversion = false;

        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateType.INSTANCE, false);
        WeeksAdd weeksAdd = new WeeksAdd(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = weeksAdd.getSignature();

        Assertions.assertEquals(DateType.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testNonExplicitCastToDateV2TypeUsesDateV2SignatureForWeeksSub() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateV2Type.INSTANCE, false);
        WeeksSub weeksSub = new WeeksSub(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = weeksSub.getSignature();

        Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testNonExplicitCastToDateTypeUsesDateSignatureForWeeksSub() {
        Config.enable_date_conversion = false;

        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateType.INSTANCE, false);
        WeeksSub weeksSub = new WeeksSub(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = weeksSub.getSignature();

        Assertions.assertEquals(DateType.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testSysdateWithWeeksAddUsesDateV2Signature() {
        // Test sysdate() override from ComputeSignatureForDateArithmetic
        SysDate sysdate = new SysDate();
        WeeksAdd weeksAdd = new WeeksAdd(sysdate, new IntegerLiteral(1));

        FunctionSignature signature = weeksAdd.getSignature();

        Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testSysdateWithWeeksSubUsesDateV2Signature() {
        // Test sysdate() override from ComputeSignatureForDateArithmetic
        SysDate sysdate = new SysDate();
        WeeksSub weeksSub = new WeeksSub(sysdate, new IntegerLiteral(1));

        FunctionSignature signature = weeksSub.getSignature();

        Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }
}
