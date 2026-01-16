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

package org.apache.doris.qe;

import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.ShowVariablesStmt;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.VariableAnnotation;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.nereids.parser.Dialect;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public class SessionVariablesTest extends TestWithFeService {

    private SessionVariable sessionVariable;
    private int numOfForwardVars;
    private ProfileManager profileManager = ProfileManager.getInstance();

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("test_d");
        useDatabase("test_d");
        createTable("create table test_t1 \n" + "(k1 int, k2 int) distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\" = \"1\");");

        sessionVariable = new SessionVariable();
        Field[] fields = SessionVariable.class.getDeclaredFields();
        for (Field f : fields) {
            VariableMgr.VarAttr varAttr = f.getAnnotation(VariableMgr.VarAttr.class);
            if (varAttr == null || !varAttr.needForward()) {
                continue;
            }
            numOfForwardVars++;
        }
    }

    @Test
    public void testExperimentalSessionVariables() throws Exception {
        connectContext.setThreadLocalInfo();
        // 1. set without experimental
        SessionVariable sessionVar = connectContext.getSessionVariable();
        boolean enableNereids = sessionVar.isEnableNereidsPlanner();
        String sql = "set enable_nereids_planner=" + (enableNereids ? "false" : "true");
        SetStmt setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        SetExecutor setExecutor = new SetExecutor(connectContext, setStmt);
        setExecutor.execute();
        Assert.assertNotEquals(sessionVar.isEnableNereidsPlanner(), enableNereids);
        // 2. set with experimental
        enableNereids = sessionVar.isEnableNereidsPlanner();
        sql = "set experimental_enable_nereids_planner=" + (enableNereids ? "false" : "true");
        setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        setExecutor = new SetExecutor(connectContext, setStmt);
        setExecutor.execute();
        Assert.assertNotEquals(sessionVar.isEnableNereidsPlanner(), enableNereids);
        // 3. set global without experimental
        enableNereids = sessionVar.isEnableNereidsPlanner();
        sql = "set global enable_nereids_planner=" + (enableNereids ? "false" : "true");
        setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        setExecutor = new SetExecutor(connectContext, setStmt);
        setExecutor.execute();
        Assert.assertNotEquals(sessionVar.isEnableNereidsPlanner(), enableNereids);
        // 4. set global with experimental
        enableNereids = sessionVar.isEnableNereidsPlanner();
        sql = "set global experimental_enable_nereids_planner=" + (enableNereids ? "false" : "true");
        setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        setExecutor = new SetExecutor(connectContext, setStmt);
        setExecutor.execute();
        Assert.assertNotEquals(sessionVar.isEnableNereidsPlanner(), enableNereids);

        // 5. set experimental for EXPERIMENTAL_ONLINE var
        boolean bucketShuffle = sessionVar.isEnableBucketShuffleJoin();
        sql = "set global experimental_enable_bucket_shuffle_join=" + (bucketShuffle ? "false" : "true");
        setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        setExecutor = new SetExecutor(connectContext, setStmt);
        setExecutor.execute();
        Assert.assertNotEquals(sessionVar.isEnableBucketShuffleJoin(), bucketShuffle);

        // 6. set non experimental for EXPERIMENTAL_ONLINE var
        bucketShuffle = sessionVar.isEnableBucketShuffleJoin();
        sql = "set global enable_bucket_shuffle_join=" + (bucketShuffle ? "false" : "true");
        setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        setExecutor = new SetExecutor(connectContext, setStmt);
        setExecutor.execute();
        Assert.assertNotEquals(sessionVar.isEnableBucketShuffleJoin(), bucketShuffle);

        // 4. set experimental for none experimental var
        sql = "set experimental_repeat_max_num=5";
        setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        SetExecutor setExecutor2 = new SetExecutor(connectContext, setStmt);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Unknown system variable",
                () -> setExecutor2.execute());

        // 5. show variables
        String showSql = "show variables like '%experimental%'";
        ShowVariablesStmt showStmt = (ShowVariablesStmt) parseAndAnalyzeStmt(showSql, connectContext);
        PatternMatcher matcher = null;
        if (showStmt.getPattern() != null) {
            matcher = PatternMatcherWrapper.createMysqlPattern(showStmt.getPattern(),
                    CaseSensibility.VARIABLES.getCaseSensibility());
        }
        int num = sessionVar.getVariableNumByVariableAnnotation(VariableAnnotation.EXPERIMENTAL);
        List<List<String>> result = VariableMgr.dump(showStmt.getType(), sessionVar, matcher);
        Assert.assertEquals(num, result.size());
    }

    @Test
    public void testForwardSessionVariables() {
        Map<String, String> vars = sessionVariable.getForwardVariables();
        Assertions.assertTrue(numOfForwardVars >= 6);
        Assertions.assertEquals(numOfForwardVars, vars.size());

        vars.put(SessionVariable.ENABLE_PROFILE, "true");
        sessionVariable.setForwardedSessionVariables(vars);
        Assertions.assertEquals(true, sessionVariable.enableProfile);
    }

    @Test
    public void testForwardQueryOptions() {
        TQueryOptions queryOptions = sessionVariable.getQueryOptionVariables();
        Assertions.assertTrue(queryOptions.isSetMemLimit());
        Assertions.assertFalse(queryOptions.isSetLoadMemLimit());
        Assertions.assertTrue(queryOptions.isSetQueryTimeout());

        queryOptions.setQueryTimeout(123);
        queryOptions.setInsertTimeout(123);
        sessionVariable.setForwardedSessionVariables(queryOptions);
        Assertions.assertEquals(123, sessionVariable.getQueryTimeoutS());
        Assertions.assertEquals(123, sessionVariable.getInsertTimeoutS());
    }

    @Test
    public void testCloneSessionVariablesWithSessionOriginValueNotEmpty() throws NoSuchFieldException {
        Field txIsolation = SessionVariable.class.getField("txIsolation");
        SessionVariableField txIsolationSessionVariableField = new SessionVariableField(txIsolation);
        sessionVariable.addSessionOriginValue(txIsolationSessionVariableField, "test");

        SessionVariable sessionVariableClone = VariableMgr.cloneSessionVariable(sessionVariable);

        Assertions.assertEquals("test",
                sessionVariableClone.getSessionOriginValue().get(txIsolationSessionVariableField));
    }

    @Test
    public void testDecimalSessionVariables() {
        sessionVariable.setSqlDialect(Dialect.PRESTO.getDialectName());
        Assertions.assertTrue(sessionVariable.isEnableDecimal256());
        sessionVariable.setSqlDialect(Dialect.DORIS.getDialectName());
        Assertions.assertFalse(sessionVariable.isEnableDecimal256());
    }

    @Test
    public void testHudiInitReaderTimeoutMsDefaultValue() {
        SessionVariable sv = new SessionVariable();
        // Default value is -1 internally, but getter should return half of query_timeout in milliseconds
        int queryTimeoutS = sv.getQueryTimeoutS();
        int expectedTimeoutMs = queryTimeoutS * 500; // query_timeout / 2 in milliseconds
        Assertions.assertEquals(expectedTimeoutMs, sv.getHudiInitReaderTimeoutMs());
    }

    @Test
    public void testHudiInitReaderTimeoutMsGetter() {
        SessionVariable sv = new SessionVariable();

        // Test with default query_timeout (900 seconds)
        sv.setQueryTimeoutS(900);
        sv.setHudiInitReaderTimeoutMs(-1);
        Assertions.assertEquals(450000, sv.getHudiInitReaderTimeoutMs()); // 900 * 500 = 450000 ms

        // Test with custom query_timeout
        sv.setQueryTimeoutS(600);
        Assertions.assertEquals(300000, sv.getHudiInitReaderTimeoutMs()); // 600 * 500 = 300000 ms

        // Test with explicit value (not -1)
        sv.setHudiInitReaderTimeoutMs(60000);
        Assertions.assertEquals(60000, sv.getHudiInitReaderTimeoutMs());

        // Test that explicit value is not affected by query_timeout change
        sv.setQueryTimeoutS(1200);
        Assertions.assertEquals(60000, sv.getHudiInitReaderTimeoutMs()); // Still 60000, not 600000
    }

    @Test
    public void testHudiInitReaderTimeoutMsSetter() {
        SessionVariable sv = new SessionVariable();

        // Test setting valid positive value
        sv.setHudiInitReaderTimeoutMs(30000);
        Assertions.assertEquals(30000, sv.getHudiInitReaderTimeoutMs());

        // Test setting to -1 (default)
        sv.setHudiInitReaderTimeoutMs(-1);
        int queryTimeoutS = sv.getQueryTimeoutS();
        Assertions.assertEquals(queryTimeoutS * 500, sv.getHudiInitReaderTimeoutMs());

        // Test setting via string
        sv.setHudiInitReaderTimeoutMs("60000");
        Assertions.assertEquals(60000, sv.getHudiInitReaderTimeoutMs());

        // Test setting invalid string (should not throw, but log warning)
        sv.setHudiInitReaderTimeoutMs("invalid");
        // Value should remain unchanged
        Assertions.assertEquals(60000, sv.getHudiInitReaderTimeoutMs());
    }

    @Test
    public void testHudiInitReaderTimeoutMsInQueryOptions() {
        SessionVariable sv = new SessionVariable();

        // Test default value (-1) in TQueryOptions
        TQueryOptions queryOptions = sv.getQueryOptionVariables();
        Assertions.assertTrue(queryOptions.isSetHudiInitReaderTimeoutMs());
        int queryTimeoutS = sv.getQueryTimeoutS();
        Assertions.assertEquals(queryTimeoutS * 500, queryOptions.getHudiInitReaderTimeoutMs());

        // Test explicit value in TQueryOptions
        sv.setHudiInitReaderTimeoutMs(120000);
        queryOptions = sv.getQueryOptionVariables();
        Assertions.assertEquals(120000, queryOptions.getHudiInitReaderTimeoutMs());

        // Test that changing query_timeout affects default value in TQueryOptions
        sv.setHudiInitReaderTimeoutMs(-1);
        sv.setQueryTimeoutS(300);
        queryOptions = sv.getQueryOptionVariables();
        Assertions.assertEquals(150000, queryOptions.getHudiInitReaderTimeoutMs()); // 300 * 500
    }

    @Test
    public void testHudiInitReaderTimeoutMsForward() {
        SessionVariable sv = new SessionVariable();

        // Test that hudi_init_reader_timeout_ms is in forward variables
        Map<String, String> forwardVars = sv.getForwardVariables();
        Assertions.assertTrue(forwardVars.containsKey(SessionVariable.HUDI_INIT_READER_TIMEOUT_MS));

        // Test setting via forward variables
        forwardVars.put(SessionVariable.HUDI_INIT_READER_TIMEOUT_MS, "90000");
        sv.setForwardedSessionVariables(forwardVars);
        Assertions.assertEquals(90000, sv.getHudiInitReaderTimeoutMs());
    }

    @Test
    public void testHudiInitReaderTimeoutMsWithQueryTimeoutChange() {
        SessionVariable sv = new SessionVariable();

        // Set to default (-1)
        sv.setHudiInitReaderTimeoutMs(-1);
        sv.setQueryTimeoutS(600);
        Assertions.assertEquals(300000, sv.getHudiInitReaderTimeoutMs()); // 600 * 500

        // Change query_timeout, default should update
        sv.setQueryTimeoutS(1200);
        Assertions.assertEquals(600000, sv.getHudiInitReaderTimeoutMs()); // 1200 * 500

        // Set explicit value, should not change with query_timeout
        sv.setHudiInitReaderTimeoutMs(100000);
        sv.setQueryTimeoutS(2000);
        Assertions.assertEquals(100000, sv.getHudiInitReaderTimeoutMs()); // Still 100000
    }
}
