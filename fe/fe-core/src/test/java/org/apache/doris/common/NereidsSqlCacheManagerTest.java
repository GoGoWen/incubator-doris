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

package org.apache.doris.common;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class NereidsSqlCacheManagerTest {

    private ConnectContext mockContext;
    private CatalogIf<?> mockCatalog;

    @BeforeEach
    public void setUp() {
        mockContext = Mockito.mock(ConnectContext.class);
        mockCatalog = Mockito.mock(InternalCatalog.class);
    }

    @Test
    public void testGenerateCacheKey_NormalCase() {
        Mockito.when(mockCatalog.getName()).thenReturn("default_catalog");
        Mockito.when(mockContext.getCurrentCatalog()).thenReturn(mockCatalog);
        Mockito.when(mockContext.getDatabase()).thenReturn("sales_db");
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("john", "%");
        Mockito.when(mockContext.getCurrentUserIdentity()).thenReturn(user);

        String key = generateCacheKey(mockContext, "SELECT * FROM orders");
        Assertions.assertEquals("default_catalog.sales_db:'john'@'%':SELECT * FROM orders", key);
    }

    @Test
    public void testGenerateCacheKey_NullCatalog() {
        Mockito.when(mockContext.getCurrentCatalog()).thenReturn(null);
        Mockito.when(mockContext.getDatabase()).thenReturn("test_db");
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("guest", "localhost");
        Mockito.when(mockContext.getCurrentUserIdentity()).thenReturn(user);

        String key = generateCacheKey(mockContext, "SELECT 1");
        Assertions.assertEquals(".test_db:'guest'@'localhost':SELECT 1", key);
    }

    @Test
    public void testGenerateCacheKey_NullDatabase() {
        Mockito.when(mockCatalog.getName()).thenReturn("inventory");
        Mockito.when(mockContext.getCurrentCatalog()).thenReturn(mockCatalog);
        Mockito.when(mockContext.getDatabase()).thenReturn(null);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("admin", "10.0.0.%");
        Mockito.when(mockContext.getCurrentUserIdentity()).thenReturn(user);

        String key = generateCacheKey(mockContext, "DELETE FROM items");
        Assertions.assertEquals("inventory.:'admin'@'10.0.0.%':DELETE FROM items", key);
    }

    @Test
    public void testGenerateCacheKey_SpecialCharacters() {
        Mockito.when(mockCatalog.getName()).thenReturn("catalog");
        Mockito.when(mockContext.getCurrentCatalog()).thenReturn(mockCatalog);
        Mockito.when(mockContext.getDatabase()).thenReturn("metrics_db");
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("service_account", "192.168.0.0/24");
        Mockito.when(mockContext.getCurrentUserIdentity()).thenReturn(user);

        String key = generateCacheKey(mockContext, "UPDATE `table` SET val='@'");
        Assertions.assertEquals("catalog.metrics_db:'service_account'@'192.168.0.0/24':UPDATE `table` SET val='@'", key);
    }

    private String generateCacheKey(ConnectContext ctx, String sql) {
        CatalogIf<?> catalog = ctx.getCurrentCatalog();
        String catalogName = catalog != null ? catalog.getName() : "";
        String dbName = ctx.getDatabase() != null ? ctx.getDatabase() : "";
        return catalogName + "." + dbName + ":"
               + ctx.getCurrentUserIdentity().toString() + ":" + sql;
    }
}
