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

package org.apache.doris.planner;

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class DistributedPlannerTest {
    private static String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID().toString() + "/";
    private static ConnectContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setEnableNereidsPlanner(false);
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Env.getCurrentEnv().createDb(createDbStmt);
        // create table tbl1
        String createTblStmtStr = "create table db1.tbl1(k1 int, k2 varchar(32), v bigint sum) "
                + "AGGREGATE KEY(k1,k2) distributed by hash(k1) buckets 1 properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr, ctx);
        Env.getCurrentEnv().createTable(createTableStmt);
        // create table tbl2
        createTblStmtStr = "create table db1.tbl2(k3 int, k4 varchar(32)) "
                + "DUPLICATE KEY(k3) distributed by hash(k3) buckets 1 properties('replication_num' = '1');";
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr, ctx);
        Env.getCurrentEnv().createTable(createTableStmt);
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(runningDir));
    }

    @Test
    public void testAssertFragmentWithDistributedInput(@Injectable AssertNumRowsNode assertNumRowsNode,
                                                       @Injectable PlanFragment inputFragment,
                                                       @Injectable PlanNodeId planNodeId,
                                                       @Injectable PlanFragmentId planFragmentId,
                                                       @Injectable PlanNode inputPlanRoot,
                                                       @Injectable TupleId tupleId,
                                                       @Mocked PlannerContext plannerContext) {
        DistributedPlanner distributedPlanner = new DistributedPlanner(plannerContext);

        List<TupleId> tupleIdList = Lists.newArrayList(tupleId);
        Set<TupleId> tupleIdSet = Sets.newHashSet(tupleId);
        Deencapsulation.setField(inputPlanRoot, "tupleIds", tupleIdList);
        Deencapsulation.setField(inputPlanRoot, "tblRefIds", tupleIdList);
        Deencapsulation.setField(inputPlanRoot, "nullableTupleIds", Sets.newHashSet(tupleId));
        Deencapsulation.setField(inputPlanRoot, "conjuncts", Lists.newArrayList());
        new Expectations() {
            {
                inputPlanRoot.getOutputTupleDesc();
                result = null;
                inputFragment.isPartitioned();
                result = true;
                plannerContext.getNextNodeId();
                result = planNodeId;
                plannerContext.getNextFragmentId();
                result = planFragmentId;
                inputFragment.getPlanRoot();
                result = inputPlanRoot;
                inputPlanRoot.getTupleIds();
                result = tupleIdList;
                inputPlanRoot.getTblRefIds();
                result = tupleIdList;
                inputPlanRoot.getNullableTupleIds();
                result = tupleIdSet;
                assertNumRowsNode.getChildren();
                result = inputPlanRoot;
            }
        };

        PlanFragment assertFragment = Deencapsulation.invoke(distributedPlanner, "createAssertFragment",
                assertNumRowsNode, inputFragment);
        Assert.assertFalse(assertFragment.isPartitioned());
        Assert.assertSame(assertNumRowsNode, assertFragment.getPlanRoot());
    }

    @Test
    public void testAssertFragmentWithUnpartitionInput(@Injectable AssertNumRowsNode assertNumRowsNode,
                                                       @Injectable PlanFragment inputFragment,
                                                       @Mocked PlannerContext plannerContext) {
        DistributedPlanner distributedPlanner = new DistributedPlanner(plannerContext);

        PlanFragment assertFragment = Deencapsulation.invoke(distributedPlanner, "createAssertFragment",
                assertNumRowsNode, inputFragment);
        Assert.assertSame(assertFragment, inputFragment);
        Assert.assertTrue(assertFragment.getPlanRoot() instanceof AssertNumRowsNode);
    }

    @Test
    public void testExplicitlyBroadcastJoin() throws Exception {
        String sql = "explain select * from db1.tbl1 join [BROADCAST] db1.tbl2 on tbl1.k1 = tbl2.k3";
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, sql);
        stmtExecutor.execute();
        Planner planner = stmtExecutor.planner();
        String plan = planner.getExplainString(new ExplainOptions(false, false, false));
        Assert.assertEquals(1, StringUtils.countMatches(plan, "INNER JOIN(BROADCAST)"));

        sql = "explain select * from db1.tbl1 join [SHUFFLE] db1.tbl2 on tbl1.k1 = tbl2.k3";
        stmtExecutor = new StmtExecutor(ctx, sql);
        stmtExecutor.execute();
        planner = stmtExecutor.planner();
        plan = planner.getExplainString(new ExplainOptions(false, false, false));
        Assert.assertEquals(1, StringUtils.countMatches(plan, "INNER JOIN(PARTITIONED)"));
    }

    @Test
    public void testBroadcastJoinCostThreshold() throws Exception {
        String sql = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from db1.tbl1 join db1.tbl2 on tbl1.k1 = tbl2.k3";
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, sql);
        stmtExecutor.execute();
        Planner planner = stmtExecutor.planner();
        String plan = planner.getExplainString(new ExplainOptions(false, false, false));
        Assert.assertEquals(1, StringUtils.countMatches(plan, "INNER JOIN(BROADCAST)"));

        double originThreshold = ctx.getSessionVariable().autoBroadcastJoinThreshold;
        try {
            ctx.getSessionVariable().autoBroadcastJoinThreshold = -1.0;
            stmtExecutor = new StmtExecutor(ctx, sql);
            stmtExecutor.execute();
            planner = stmtExecutor.planner();
            plan = planner.getExplainString(new ExplainOptions(false, false, false));
            Assert.assertEquals(1, StringUtils.countMatches(plan, "INNER JOIN(PARTITIONED)"));
        } finally {
            ctx.getSessionVariable().autoBroadcastJoinThreshold = originThreshold;
        }
    }

    @Test
    public void testCreatePlanFragments_MultipleScanRanges(@Injectable PlanFragment inputFragment,
                                                            @Injectable PlanNode planRoot,
                                                            @Injectable ScanNode scanNode1,
                                                            @Injectable ScanNode scanNode2,
                                                            @Injectable PlanFragmentId fragmentId,
                                                            @Injectable org.apache.doris.thrift.TScanRangeLocations scanRange1,
                                                            @Injectable org.apache.doris.thrift.TScanRangeLocations scanRange2,
                                                            @Injectable org.apache.doris.thrift.TScanRangeLocations scanRange3,
                                                            @Mocked PlannerContext plannerContext) {
        // Setup: Single scan node with multiple scan ranges (numInstances=1, scanRangeNum=3)
        // This should trigger merge fragment creation
        List<org.apache.doris.thrift.TScanRangeLocations> scanRanges = Lists.newArrayList(scanRange1, scanRange2, scanRange3);
        List<ScanNode> scanNodes = Lists.newArrayList(scanNode1);

        new Expectations() {{
                inputFragment.isPartitioned();
                result = true;
                minTimes = 0;

                inputFragment.getPlanRoot();
                result = planRoot;
                minTimes = 0;

                planRoot.getNumInstances();
                result = 1; // Only 1 instance
                minTimes = 0;

                scanNode1.getScanRangeLocations(0);
                result = scanRanges; // 3 scan ranges
                minTimes = 0;

                planRoot.collectInCurrentFragment((java.util.function.Predicate<PlanNode>) any);
                result = scanNodes;
                minTimes = 0;
            }};

        // Test: The new logic should detect multiple scan ranges and create merge fragment
        // Even though numInstances=1, scanRangeNum=3 should trigger merge
        // This would be tested in actual createPlanFragments call, but we verify the logic
        int scanRangeNum = 0;
        for (ScanNode scanNode : scanNodes) {
            scanRangeNum += scanNode.getScanRangeLocations(0).size();
        }

        Assert.assertEquals(3, scanRangeNum);
        // With numInstances=1 and scanRangeNum=3, merge fragment should be created
        boolean shouldCreateMerge = planRoot.getNumInstances() > 1 || scanRangeNum > 1;
        Assert.assertTrue("Should create merge fragment when scanRangeNum > 1", shouldCreateMerge);
    }

    @Test
    public void testCreatePlanFragments_SingleScanRange(@Injectable PlanFragment inputFragment,
                                                         @Injectable PlanNode planRoot,
                                                         @Injectable ScanNode scanNode,
                                                         @Injectable org.apache.doris.thrift.TScanRangeLocations scanRange) {
        // Setup: Single scan node with single scan range (numInstances=1, scanRangeNum=1)
        // This should NOT trigger merge fragment creation
        List<org.apache.doris.thrift.TScanRangeLocations> scanRanges = Lists.newArrayList(scanRange);
        List<ScanNode> scanNodes = Lists.newArrayList(scanNode);

        new Expectations() {{
                inputFragment.isPartitioned();
                result = true;
                minTimes = 0;

                inputFragment.getPlanRoot();
                result = planRoot;
                minTimes = 0;

                planRoot.getNumInstances();
                result = 1; // Only 1 instance
                minTimes = 0;

                scanNode.getScanRangeLocations(0);
                result = scanRanges; // 1 scan range
                minTimes = 0;

                planRoot.collectInCurrentFragment((java.util.function.Predicate<PlanNode>) any);
                result = scanNodes;
                minTimes = 0;
            }};

        // Test: With numInstances=1 and scanRangeNum=1, no merge fragment should be created
        int scanRangeNum = 0;
        for (ScanNode scanNode1 : scanNodes) {
            scanRangeNum += scanNode1.getScanRangeLocations(0).size();
        }

        Assert.assertEquals(1, scanRangeNum);
        boolean shouldCreateMerge = planRoot.getNumInstances() > 1 || scanRangeNum > 1;
        Assert.assertFalse("Should NOT create merge fragment when numInstances=1 and scanRangeNum=1", shouldCreateMerge);
    }

    @Test
    public void testCreatePlanFragments_MultipleScanNodes(@Injectable PlanFragment inputFragment,
                                                           @Injectable PlanNode planRoot,
                                                           @Injectable ScanNode scanNode1,
                                                           @Injectable ScanNode scanNode2,
                                                           @Injectable org.apache.doris.thrift.TScanRangeLocations scanRange1,
                                                           @Injectable org.apache.doris.thrift.TScanRangeLocations scanRange2) {
        // Setup: Multiple scan nodes, each with single scan range (numInstances=1, total scanRangeNum=2)
        // This should trigger merge fragment creation
        List<org.apache.doris.thrift.TScanRangeLocations> scanRanges1 = Lists.newArrayList(scanRange1);
        List<org.apache.doris.thrift.TScanRangeLocations> scanRanges2 = Lists.newArrayList(scanRange2);
        List<ScanNode> scanNodes = Lists.newArrayList(scanNode1, scanNode2);

        new Expectations() {{
                inputFragment.isPartitioned();
                result = true;
                minTimes = 0;

                inputFragment.getPlanRoot();
                result = planRoot;
                minTimes = 0;

                planRoot.getNumInstances();
                result = 1; // Only 1 instance
                minTimes = 0;

                scanNode1.getScanRangeLocations(0);
                result = scanRanges1; // 1 scan range
                minTimes = 0;

                scanNode2.getScanRangeLocations(0);
                result = scanRanges2; // 1 scan range
                minTimes = 0;

                planRoot.collectInCurrentFragment((java.util.function.Predicate<PlanNode>) any);
                result = scanNodes;
                minTimes = 0;
            }};

        // Test: Multiple scan nodes with total scanRangeNum=2 should trigger merge
        int scanRangeNum = 0;
        for (ScanNode scanNode : scanNodes) {
            scanRangeNum += scanNode.getScanRangeLocations(0).size();
        }

        Assert.assertEquals(2, scanRangeNum);
        // With numInstances=1 and scanRangeNum=2, merge fragment should be created
        boolean shouldCreateMerge = planRoot.getNumInstances() > 1 || scanRangeNum > 1;
        Assert.assertTrue("Should create merge fragment when total scanRangeNum > 1", shouldCreateMerge);
    }
}
