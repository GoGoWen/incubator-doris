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

package org.apache.doris.datasource.hive;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class HMSExternalTableTest {
    @Test
    public void testGetNullParamters(@Injectable Table remoteTable) {
        new Expectations() {
            {
                remoteTable.getParameters();
                result = null;
            }
        };

        new MockUp<HMSExternalTable>() {
            @Mock
            public final synchronized void makeSureInitialized() {
            }
        };

        HMSExternalTable hmsExternalTable = new HMSExternalTable(1, "test", "test",
                new HMSExternalCatalog());
        hmsExternalTable.setRemoteTable(remoteTable);
        Expression policy = hmsExternalTable.getRowPolicy();
        Assertions.assertNull(policy);
    }

    @Test
    public void testGetNullRowPolicy(@Injectable Table remoteTable) {
        new Expectations() {
            {
                remoteTable.getParameters();
                result = Maps.newHashMap();
            }
        };

        new MockUp<HMSExternalTable>() {
            @Mock
            public final synchronized void makeSureInitialized() {
            }
        };

        HMSExternalTable hmsExternalTable = new HMSExternalTable(1, "test", "test",
                new HMSExternalCatalog());
        hmsExternalTable.setRemoteTable(remoteTable);
        Expression policy = hmsExternalTable.getRowPolicy();
        Assertions.assertNull(policy);
    }


    @Test
    public void testGetNormalRowPolicy(@Injectable Table remoteTable) {
        Map<String, String> map = Maps.newHashMap();
        map.put("row_policy", "(id < 1)");
        new Expectations() {
            {
                remoteTable.getParameters();
                result = map;
            }
        };

        new MockUp<HMSExternalTable>() {
            @Mock
            public final synchronized void makeSureInitialized() {
            }
        };

        HMSExternalTable hmsExternalTable = new HMSExternalTable(1, "test", "test",
                new HMSExternalCatalog());
        hmsExternalTable.setRemoteTable(remoteTable);
        Expression policy = hmsExternalTable.getRowPolicy();
        Assertions.assertTrue(policy instanceof LessThan);
        Assertions.assertEquals("(id < 1)", policy.toSql());
    }
}
