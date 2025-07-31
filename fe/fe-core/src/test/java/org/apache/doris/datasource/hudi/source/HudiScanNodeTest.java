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

package org.apache.doris.datasource.hudi.source;

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hudi.common.storage.HoodieDefaultStorageStrategy;
import org.apache.hudi.common.storage.HoodieStorageStrategy;
import org.apache.hudi.common.storage.HoodieStorageStrategyFactory;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.org.apache.avro.Schema;
import org.apache.hudi.org.apache.avro.Schema.Field;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HudiScanNodeTest {

    @Test
    public void testDoInitialize(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc, @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog, @Injectable HoodieTableMetaClient client,
            @Injectable org.apache.hadoop.hive.metastore.api.Table hiveTable,
            @Injectable StorageDescriptor storageDescriptor, @Injectable SerDeInfo serDeInfo) {
        Map<String, String> para1 = Maps.newHashMap();
        para1.put("hoodie.query.without.cache.layer.enabled", "true");
        Map<String, String> para2 = Maps.newHashMap();
        para2.put("hoodie.datasource.write.recordkey.field", "id,name");
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                table.isHoodieCowTable();
                result = true;

                catalog.bindBrokerName();
                result = "test";

                table.useHiveSyncPartition();
                result = true;

                table.isView();
                result = false;

                client.reloadActiveTimeline();

                table.getRemoteTable();
                result = hiveTable;

                hiveTable.getSd();
                result = storageDescriptor;

                storageDescriptor.getLocation();
                result = "hdfs://test";

                storageDescriptor.getInputFormat();
                result = "org.apache.hudi.hadoop.HoodieParquetInputFormat";

                storageDescriptor.getSerdeInfo();
                result =  serDeInfo;

                serDeInfo.getSerializationLib();
                result = "org.apache.hudi.hive.HoodieHiveSerDe";

                serDeInfo.getParameters();
                result = para1;

                hiveTable.getParameters();
                result = para2;
            }
        };
        new MockUp<HudiScanNode>() {
            @Mock
            public void computeColumnsFilter() {

            }

            @Mock
            public void initBackendPolicy() {

            }

            @Mock
            public void initSchemaParams() {

            }

            @Mock
            public TableSnapshot getQueryTableSnapshot() {
                return null;
            }

        };

        new MockUp<HiveMetaStoreClientHelper>() {
            @Mock
            public HoodieTableMetaClient getHudiClient(HMSExternalTable table)  {
                return client;
            }
        };

        new MockUp<HoodieStorageStrategyFactory>() {
            @Mock
            public HoodieStorageStrategy getInstant(HoodieTableMetaClient client, boolean reset) {
                return new HoodieDefaultStorageStrategy("", "", Option.of(client));
            }
        };

        MockedStatic<HoodieStorageStrategyFactory> mocked = Mockito.mockStatic(HoodieStorageStrategyFactory.class);
        mocked.when(() -> HoodieStorageStrategyFactory.getInstant(Mockito.any(), Mockito.anyBoolean())).thenReturn(
                new HoodieDefaultStorageStrategy("hdfs://ns10000/test",
                        "hdfs://ns10000/test/test", Option.of(client)));

        new MockUp<TableSchemaResolver>() {
            @Mock
            public Schema getTableAvroSchema() {
                List<Field> fieldList = new ArrayList<>();
                return Schema.createRecord(fieldList);
            }
        };
        try {
            HudiScanNode scanNode = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                    false, Optional.empty(), Optional.empty(), sessionVariable);
            scanNode.doInitialize();
            String[] primaryKeys = new String[]{"id", "name"};
            Assertions.assertEquals(primaryKeys.length, scanNode.getPrimaryKeys().size());
            Assertions.assertEquals(primaryKeys[0], scanNode.getPrimaryKeys().get(0));
            Assertions.assertEquals(primaryKeys[1], scanNode.getPrimaryKeys().get(1));
        } catch (Exception e) {
            Assertions.assertEquals("", ExceptionUtils.getStackTrace(e));
            Assertions.fail(e);
        }
    }
}
