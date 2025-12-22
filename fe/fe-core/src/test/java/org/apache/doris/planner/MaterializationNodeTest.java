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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MaterializationNodeTest {

    private MaterializationNode materializationNode;

    @Mock private TupleDescriptor materializeTupleDescriptor;
    @Mock private TupleDescriptor outputTupleDesc;
    @Mock private PlanNode childNode;
    @Mock private ConnectContext context;
    @Mock private Env env;
    @Mock private Backend backend;
    @Mock private Expr expr;
    @Mock private SystemInfoService mockSystemInfoService;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Mockito.when(materializeTupleDescriptor.getId()).thenReturn(new TupleId(0));
        materializationNode = new MaterializationNode(new PlanNodeId(0), materializeTupleDescriptor, childNode);
    }

    @Test
    public void testMaterializationNodeConstructor() {
        Assert.assertNotNull(materializationNode);
        Assert.assertEquals(new PlanNodeId(0), materializationNode.getId());
        Assert.assertEquals(materializeTupleDescriptor, materializationNode.getMaterializeTupleDescriptor());
        Assert.assertEquals(1, materializationNode.getChildren().size());
        Assert.assertEquals(childNode, materializationNode.getChildren().get(0));
    }

    @Test
    public void testInitNodeInfo() {
        try (org.mockito.MockedStatic<ConnectContext> utilConnectContextMock = Mockito.mockStatic(ConnectContext.class);
             org.mockito.MockedStatic<Env> utilEnvMock = Mockito.mockStatic(Env.class)) {

            utilConnectContextMock.when(() -> ConnectContext.get()).thenReturn(context);


            Mockito.when(backend.getId()).thenReturn(1000L);
            Mockito.when(backend.getHost()).thenReturn("localhost");
            Mockito.when(backend.getBrpcPort()).thenReturn(8060);

            ImmutableMap<Long, Backend> backendMap = ImmutableMap.of(1000L, backend);
            utilEnvMock.when(() -> Env.getCurrentSystemInfo()).thenReturn(mockSystemInfoService);
            utilEnvMock.when(() -> mockSystemInfoService.getIdToBackend()).thenReturn(backendMap);

            materializationNode.initNodeInfo();
            Assert.assertNotNull(materializationNode.getNodesInfo());
        }
    }

    @Test
    public void testSettersAndGetters() {
        List<Expr> rowIds = Arrays.asList(expr, expr);
        materializationNode.setRowIds(rowIds);
        Assert.assertEquals(rowIds, materializationNode.getRowIds());

        List<List<Column>> lazyColumns = new ArrayList<>();
        lazyColumns.add(Arrays.asList(new Column("col1", PrimitiveType.INT)));
        materializationNode.setLazyColumns(lazyColumns);
        Assert.assertEquals(lazyColumns, materializationNode.getLazyColumns());

        List<List<Integer>> locations = new ArrayList<>();
        locations.add(Arrays.asList(1, 2));
        materializationNode.setLocations(locations);
        Assert.assertEquals(locations, materializationNode.getLocations());

        List<List<Integer>> idxs = new ArrayList<>();
        idxs.add(Arrays.asList(3, 4));
        materializationNode.setIdxs(idxs);
        Assert.assertEquals(idxs, materializationNode.getIdxs());

        List<Boolean> rowStoreFlags = Arrays.asList(true, false);
        materializationNode.setRowStoreFlags(rowStoreFlags);
        Assert.assertEquals(rowStoreFlags, materializationNode.getRowStoreFlags());

        materializationNode.setTopMaterializeNode(true);
        Assert.assertTrue(materializationNode.isTopMaterializeNode());
    }

    @Test
    public void testToThrift() {
        try (org.mockito.MockedStatic<ConnectContext> utilConnectContextMock = Mockito.mockStatic(ConnectContext.class);
             org.mockito.MockedStatic<Env> utilEnvMock = Mockito.mockStatic(Env.class);
             org.mockito.MockedStatic<Expr> utilExprMock = Mockito.mockStatic(Expr.class);) {

            utilConnectContextMock.when(() -> ConnectContext.get()).thenReturn(context);

            Mockito.when(backend.getId()).thenReturn(1000L);
            Mockito.when(backend.getHost()).thenReturn("localhost");
            Mockito.when(backend.getBrpcPort()).thenReturn(8060);

            ImmutableMap<Long, Backend> backendMap = ImmutableMap.of(1000L, backend);
            utilEnvMock.when(() -> Env.getCurrentSystemInfo()).thenReturn(mockSystemInfoService);
            utilEnvMock.when(() -> mockSystemInfoService.getIdToBackend()).thenReturn(backendMap);

            materializationNode.initNodeInfo(); // Ensure nodesInfo is not null

            // Set up some data for the node
            List<Expr> rowIds = Arrays.asList(expr);
            materializationNode.setRowIds(rowIds);

            List<org.apache.doris.thrift.TExpr> thriftExprList = new ArrayList<>();
            utilExprMock.when(() -> Expr.treesToThrift(rowIds)).thenReturn(thriftExprList);

            List<List<Column>> lazyColumns = new ArrayList<>();
            lazyColumns.add(Arrays.asList(new Column("col1", PrimitiveType.INT)));
            materializationNode.setLazyColumns(lazyColumns);

            List<List<Integer>> locations = new ArrayList<>();
            locations.add(Arrays.asList(1, 2));
            materializationNode.setLocations(locations);

            List<List<Integer>> idxs = new ArrayList<>();
            idxs.add(Arrays.asList(3, 4));
            materializationNode.setIdxs(idxs);

            List<Boolean> rowStoreFlags = Arrays.asList(true);
            materializationNode.setRowStoreFlags(rowStoreFlags);

            materializationNode.setTopMaterializeNode(true);

            TPlanNode tPlanNode = new TPlanNode();
            materializationNode.toThrift(tPlanNode);

            Assert.assertEquals(TPlanNodeType.MATERIALIZATION_NODE, tPlanNode.node_type);
            Assert.assertNotNull(tPlanNode.materialization_node);
            Assert.assertEquals(materializationNode.getId().asInt(), tPlanNode.materialization_node.getTupleId());
            Assert.assertEquals(materializeTupleDescriptor.getId().asInt(), tPlanNode.materialization_node.getIntermediateTupleId());
            Assert.assertNotNull(tPlanNode.materialization_node.getNodesInfo());
            Assert.assertEquals(thriftExprList, tPlanNode.materialization_node.getFetchExprLists());
            Assert.assertEquals(1, tPlanNode.materialization_node.getColumnDescsLists().size());
            Assert.assertEquals(1, tPlanNode.materialization_node.getColumnDescsLists().get(0).size());
            Assert.assertEquals("col1", tPlanNode.materialization_node.getColumnDescsLists().get(0).get(0).getColumnName());
            Assert.assertEquals(locations, tPlanNode.materialization_node.getSlotLocsLists());
            Assert.assertEquals(idxs, tPlanNode.materialization_node.getColumnIdxsLists());
            Assert.assertEquals(rowStoreFlags, tPlanNode.materialization_node.getFetchRowStores());
            Assert.assertTrue(tPlanNode.materialization_node.isGcIdMap());
        }
    }

    @Test
    public void testGetNodeExplainString() {
        try (org.mockito.MockedStatic<ConnectContext> utilConnectContextMock = Mockito.mockStatic(ConnectContext.class);
             org.mockito.MockedStatic<Env> utilEnvMock = Mockito.mockStatic(Env.class)) {

            utilConnectContextMock.when(ConnectContext::get).thenReturn(context);

            Mockito.when(backend.getId()).thenReturn(1000L);
            Mockito.when(backend.getHost()).thenReturn("localhost");
            Mockito.when(backend.getBrpcPort()).thenReturn(8060);

            ImmutableMap<Long, Backend> backendMap = ImmutableMap.of(1000L, backend);
            utilEnvMock.when(() -> Env.getCurrentSystemInfo()).thenReturn(mockSystemInfoService);
            utilEnvMock.when(() -> mockSystemInfoService.getIdToBackend()).thenReturn(backendMap);

            materializationNode.initNodeInfo(); // Ensure nodesInfo is not null

            // Set up some data for the node
            List<Expr> rowIds = Arrays.asList(expr);
            materializationNode.setRowIds(rowIds);

            List<List<Column>> lazyColumns = new ArrayList<>();
            lazyColumns.add(Arrays.asList(new Column("col1", PrimitiveType.INT)));
            materializationNode.setLazyColumns(lazyColumns);

            List<List<Integer>> locations = new ArrayList<>();
            locations.add(Arrays.asList(1, 2));
            materializationNode.setLocations(locations);

            List<List<Integer>> idxs = new ArrayList<>();
            idxs.add(Arrays.asList(3, 4));
            materializationNode.setIdxs(idxs);

            materializationNode.setTopMaterializeNode(false);

            // Mock projectList and outputTupleDesc for full coverage of getNodeExplainString
            List<Expr> projectList = Arrays.asList(expr);
            materializationNode.projectList = projectList;
            Mockito.when(outputTupleDesc.getId()).thenReturn(new TupleId(1));
            materializationNode.outputTupleDesc = outputTupleDesc;


            String explainString = materializationNode.getNodeExplainString("", TExplainLevel.NORMAL);
            System.out.println(explainString);

            Assert.assertTrue(explainString.contains("materialize tuple id:" + materializeTupleDescriptor.getId().asInt()));
            Assert.assertTrue(explainString.contains("output tuple id:1"));
            Assert.assertTrue(explainString.contains("column_descs_lists" + lazyColumns));
            Assert.assertTrue(explainString.contains("locations: " + locations));
            Assert.assertTrue(explainString.contains("table_idxs: " + idxs));
            Assert.assertTrue(explainString.contains("row_ids: " + rowIds));
            Assert.assertTrue(explainString.contains("isTopMaterializeNode: false"));
        }
    }
}

