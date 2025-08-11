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

import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper.HudiClientKey;
import org.apache.doris.qe.BDPAuthContext;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.security.PrivilegedAction;

public class HiveMetaStoreClientHelperTest {

    @Test
    public void testHudiClientKey(@Injectable HMSExternalTable table1, @Injectable HMSExternalTable table2,
            @Injectable HMSExternalTable table3) {
        new Expectations() {
            {
                table1.getDbName();
                result = "db1";

                table1.getName();
                result = "table1";

                table2.getDbName();
                result = "db2";

                table2.getName();
                result = "table2";

                table3.getDbName();
                result = "db1";

                table3.getName();
                result = "table1";
            }
        };

        HudiClientKey key1 = new HudiClientKey("userA", "test", "test", table1);
        HudiClientKey key2 = new HudiClientKey("userB", "test", "test", table2);
        HudiClientKey key3 = new HudiClientKey("userA", "test", "test", table3);

        Assertions.assertEquals(key1, key3);
        Assertions.assertEquals(key1, key1);
        Assertions.assertNotEquals(key1, key2);
        Assertions.assertEquals(key1.hashCode(), key3.hashCode());
        Assertions.assertFalse(key1.equals(new String("test")));
        Assertions.assertEquals("HudiClientKey{hadoopUserName='userA', beeSource='test',"
                + " beeUser='test',dbName='db1', tblName='table1'}", key1.toString());
        Assertions.assertEquals("HudiClientKey{hadoopUserName='userB', beeSource='test',"
                + " beeUser='test',dbName='db2', tblName='table2'}", key2.toString());
        Assertions.assertEquals("HudiClientKey{hadoopUserName='userA', beeSource='test',"
                + " beeUser='test',dbName='db1', tblName='table1'}", key3.toString());
    }

    @Test
    public void testGetHudiClient(@Injectable HMSExternalTable table,
            @Injectable Table remoteTable, @Injectable ExternalCatalog catalog,
            @Injectable HoodieTableMetaClient client, @Injectable HoodieTableConfig tableConfig) {
        BDPAuthContext bdpAuthContext = new BDPAuthContext("test", "test", "test", "xxxxxx");
        bdpAuthContext.setThreadLocalInfo();
        StorageDescriptor storageDescriptor = new StorageDescriptor();
        storageDescriptor.setLocation("hdfs://ns123456/user/test.db/hudi_table");
        new Expectations() {
            {
                table.getRemoteTable();
                result = remoteTable;

                remoteTable.getSd();
                result = storageDescriptor;

                table.getCatalog();
                result = catalog;

                catalog.ifNotSetFallbackToSimpleAuth();
                result = true;

                table.getHadoopProperties();
                result = Maps.newHashMap();

                client.getTableConfig();
                result = tableConfig;

                tableConfig.getChubaoFsOwner();
                result = "test:test";
            }
        };

        new MockUp<UserGroupInformation>() {
            @Mock
            public HoodieTableMetaClient doAs(PrivilegedAction<HoodieTableMetaClient> action) {
                return client;
            }
        };
        HoodieTableMetaClient client1 = HiveMetaStoreClientHelper.getHudiClient(table);
        Assertions.assertEquals("test:test", client1.getTableConfig().getChubaoFsOwner());
    }

}
