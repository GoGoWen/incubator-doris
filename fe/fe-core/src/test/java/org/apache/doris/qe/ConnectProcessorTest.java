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

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.CatalogAccessController;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.thrift.TUniqueId;

import mockit.Expectations;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;

public class ConnectProcessorTest {

    private String backupRouting;
    private String backupFallbackCatalog;
    private ConnectContext ctx;

    @Before
    public void setUp() {
        backupRouting = Config.source_catalog_routing;
        backupFallbackCatalog = Config.sql_fallback_catalog;
        Config.sql_fallback_catalog = "presto";
        ctx = new ConnectContext();
        ctx.setQueryId(new TUniqueId(12345L, 67890L));
    }

    @After
    public void tearDown() {
        Config.source_catalog_routing = backupRouting;
        Config.sql_fallback_catalog = backupFallbackCatalog;
        ConnectContext.remove();
    }

    @Test
    public void testHandleQuery_forwardAfterHiveAuthCheck() throws Exception {
        Config.source_catalog_routing = "sandbox:presto";

        BDPAuthContext bdpAuthContext = new BDPAuthContext("test_erp", "sandbox", "test_user", "test_token");
        ctx.setBdpAuthContext(bdpAuthContext);

        UserIdentity currentUser = new UserIdentity("test_user", "%");
        currentUser.setIsAnalyzed();
        ctx.setCurrentUserIdentity(currentUser);
        ctx.setQualifiedUser("test_user");
        ctx.setRemoteIP("127.0.0.1");

        Env env = AccessTestUtil.fetchAdminCatalog();
        Field envField = ConnectContext.class.getDeclaredField("env");
        envField.setAccessible(true);
        envField.set(ctx, env);
        ctx.setThreadLocalInfo();

        InternalCatalog catalog = env.getInternalCatalog();
        Database db = catalog.getDbNullable("testDb");
        new Expectations(catalog) {
            {
                catalog.getDbOrAnalysisException("testDb");
                minTimes = 0;
                result = db;
            }
        };

        AccessControllerManager originalAccessManager = env.getAccessManager();

        // Use JMockit to enhance the original AccessControllerManager to allow all privilege checks
        // This ensures analyze() phase can pass privilege checks
        new Expectations(originalAccessManager) {
            {
                originalAccessManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString,
                        (PrivPredicate) any);
                minTimes = 0;
                result = true;

                originalAccessManager.checkTblPriv((UserIdentity) any, anyString, anyString, anyString,
                        (PrivPredicate) any);
                minTimes = 0;
                result = true;

                originalAccessManager.checkDbPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                // Mock checkTblPriv(ConnectContext, TableName, PrivPredicate) - used in SelectStmt.getTables()
                originalAccessManager.checkTblPriv((ConnectContext) any, (org.apache.doris.analysis.TableName) any,
                        (PrivPredicate) any);
                minTimes = 0;
                result = true;

                originalAccessManager.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = false; // Return false to fall through to catalog-specific checks

                originalAccessManager.checkGlobalPriv((UserIdentity) any, (PrivPredicate) any);
                minTimes = 0;
                result = false; // Return false to fall through to catalog-specific checks
            }
        };

        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        CatalogAccessController mockHiveAccessController = Mockito.mock(CatalogAccessController.class);
        CatalogAccessController mockInternalAccessController = Mockito.mock(CatalogAccessController.class);
        Mockito.when(accessManager.getAccessControllerOrDefault("hive")).thenReturn(mockHiveAccessController);
        Mockito.when(accessManager.getAccessControllerOrDefault("internal")).thenReturn(mockInternalAccessController);
        Mockito.when(accessManager.getAccessControllerOrDefault(Mockito.anyString())).thenReturn(mockInternalAccessController);
        Mockito.when(accessManager.checkGlobalPriv(Mockito.any(UserIdentity.class), Mockito.any(PrivPredicate.class))).thenReturn(false);
        Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class), Mockito.any(org.apache.doris.analysis.TableName.class),
                Mockito.any(PrivPredicate.class))).thenReturn(true);
        Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.any(PrivPredicate.class))).thenReturn(true);
        Mockito.when(accessManager.checkTblPriv(Mockito.any(UserIdentity.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.any(PrivPredicate.class))).thenReturn(true);
        Mockito.when(mockHiveAccessController.checkTblPriv(
                Mockito.any(Boolean.class), Mockito.any(UserIdentity.class), Mockito.eq("hive"), Mockito.anyString(), Mockito.anyString(),
                Mockito.eq(PrivPredicate.SELECT))).thenReturn(true);
        Mockito.when(mockHiveAccessController.checkTblPriv(
                Mockito.any(UserIdentity.class), Mockito.eq("hive"), Mockito.anyString(), Mockito.anyString(),
                Mockito.eq(PrivPredicate.SELECT))).thenReturn(true);
        Mockito.when(mockInternalAccessController.checkTblPriv(
                Mockito.any(Boolean.class), Mockito.any(UserIdentity.class), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.any(PrivPredicate.class))).thenReturn(true);
        Mockito.when(mockInternalAccessController.checkTblPriv(
                Mockito.any(UserIdentity.class), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.any(PrivPredicate.class))).thenReturn(true);

        Field accessManagerField = Env.class.getDeclaredField("accessManager");
        accessManagerField.setAccessible(true);
        accessManagerField.set(env, accessManager);

        new Expectations(env) {
            {
                env.getAccessManager();
                minTimes = 0;
                result = accessManager;
            }
        };

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            envMock.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

            MysqlConnectProcessor processor = new MysqlConnectProcessor(ctx);
            ConnectProcessor processorSpy = Mockito.spy(processor);
            AtomicReference<String> forwardedSql = new AtomicReference<>();
            AtomicReference<Boolean> executeQueryCalled = new AtomicReference<>(false);

            Mockito.doAnswer(invocation -> {
                executeQueryCalled.set(true);
                return null;
            }).when(processorSpy).executeQuery(Mockito.any(MysqlCommand.class), Mockito.anyString());

            Mockito.doAnswer(invocation -> {
                forwardedSql.set((String) invocation.getArguments()[1]);
                ctx.getState().setOk();
                return null;
            }).when(processorSpy).fallbackExecuteQuery(Mockito.any(MysqlCommand.class), Mockito.anyString());

            Method handleQuery = ConnectProcessor.class.getDeclaredMethod(
                    "handleQuery", MysqlCommand.class, String.class);
            handleQuery.setAccessible(true);

            handleQuery.invoke(processorSpy, MysqlCommand.COM_QUERY, "select * from testDb.testTbl");
            Assert.assertNotNull("Should forward query", forwardedSql.get());
            Assert.assertTrue(forwardedSql.get().contains("query('catalog'='presto'"));
            Assert.assertFalse("executeQuery should not be called when forwarding", executeQueryCalled.get());
            Mockito.verify(accessManager, Mockito.atLeastOnce()).getAccessControllerOrDefault("hive");
        }
    }

    @Test
    public void testHandleQuery_authFailed() throws Exception {
        Config.source_catalog_routing = "sandbox:presto";

        BDPAuthContext bdpAuthContext = new BDPAuthContext("test_erp", "sandbox", "test_user", "test_token");
        ctx.setBdpAuthContext(bdpAuthContext);

        UserIdentity currentUser = new UserIdentity("test_user", "%");
        currentUser.setIsAnalyzed();
        ctx.setCurrentUserIdentity(currentUser);
        ctx.setQualifiedUser("test_user");
        ctx.setRemoteIP("127.0.0.1");

        Env env = AccessTestUtil.fetchAdminCatalog();
        Field envField = ConnectContext.class.getDeclaredField("env");
        envField.setAccessible(true);
        envField.set(ctx, env);
        ctx.setThreadLocalInfo();

        InternalCatalog catalog = env.getInternalCatalog();
        Database db = catalog.getDbNullable("testDb");
        new Expectations(catalog) {
            {
                catalog.getDbOrAnalysisException("testDb");
                minTimes = 0;
                result = db;
            }
        };

        AccessControllerManager originalAccessManager = env.getAccessManager();

        new Expectations(originalAccessManager) {
            {
                originalAccessManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString,
                        (PrivPredicate) any);
                minTimes = 0;
                result = true;

                originalAccessManager.checkTblPriv((UserIdentity) any, anyString, anyString, anyString,
                        (PrivPredicate) any);
                minTimes = 0;
                result = true;

                originalAccessManager.checkDbPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                originalAccessManager.checkTblPriv((ConnectContext) any, (org.apache.doris.analysis.TableName) any,
                        (PrivPredicate) any);
                minTimes = 0;
                result = true;

                originalAccessManager.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = false;

                originalAccessManager.checkGlobalPriv((UserIdentity) any, (PrivPredicate) any);
                minTimes = 0;
                result = false;
            }
        };

        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        CatalogAccessController mockHiveAccessController = Mockito.mock(CatalogAccessController.class);
        Mockito.when(accessManager.getAccessControllerOrDefault("hive")).thenReturn(mockHiveAccessController);
        Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class), Mockito.any(org.apache.doris.analysis.TableName.class),
                Mockito.any(PrivPredicate.class))).thenReturn(true);
        Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.any(PrivPredicate.class))).thenReturn(true);
        Mockito.when(accessManager.checkTblPriv(Mockito.any(UserIdentity.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.any(PrivPredicate.class))).thenReturn(true);
        Mockito.when(mockHiveAccessController.checkTblPriv(
                Mockito.any(UserIdentity.class), Mockito.eq("hive"), Mockito.anyString(), Mockito.anyString(),
                Mockito.eq(PrivPredicate.SELECT))).thenReturn(false);

        Field accessManagerField = Env.class.getDeclaredField("accessManager");
        accessManagerField.setAccessible(true);
        accessManagerField.set(env, accessManager);

        new Expectations(env) {
            {
                env.getAccessManager();
                minTimes = 0;
                result = accessManager;
            }
        };

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            envMock.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

            MysqlConnectProcessor processor = new MysqlConnectProcessor(ctx);
            ConnectProcessor processorSpy = Mockito.spy(processor);
            AtomicReference<Boolean> forwardedCalled = new AtomicReference<>(false);

            Mockito.doAnswer(invocation -> {
                forwardedCalled.set(true);
                return null;
            }).when(processorSpy).fallbackExecuteQuery(Mockito.any(MysqlCommand.class), Mockito.anyString());

            Method handleQuery = ConnectProcessor.class.getDeclaredMethod(
                    "handleQuery", MysqlCommand.class, String.class);
            handleQuery.setAccessible(true);

            handleQuery.invoke(processorSpy, MysqlCommand.COM_QUERY, "select * from testDb.testTbl");
            Assert.assertFalse("Should not forward when auth failed", forwardedCalled.get());
            Assert.assertEquals(MysqlStateType.ERR, ctx.getState().getStateType());
            Mockito.verify(accessManager, Mockito.atLeastOnce()).getAccessControllerOrDefault("hive");
        }
    }

    @Test
    public void testHandleQuery_noForwardWhenNoRouted() throws Exception {
        Config.source_catalog_routing = "";

        MysqlConnectProcessor processor = new MysqlConnectProcessor(ctx);
        ConnectProcessor processorSpy = Mockito.spy(processor);
        AtomicReference<Boolean> executeQueryCalled = new AtomicReference<>(false);

        Mockito.doAnswer(invocation -> {
            executeQueryCalled.set(true);
            return null;
        }).when(processorSpy).executeQuery(Mockito.any(MysqlCommand.class), Mockito.anyString());

        Method handleQuery = ConnectProcessor.class.getDeclaredMethod(
                "handleQuery", MysqlCommand.class, String.class);
        handleQuery.setAccessible(true);
        handleQuery.invoke(processorSpy, MysqlCommand.COM_QUERY, "select 1");

        Assert.assertTrue(executeQueryCalled.get());
    }

}
