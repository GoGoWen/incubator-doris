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

package org.apache.doris.catalog;

import org.apache.doris.qe.BDPAuthContext;
import org.apache.doris.thrift.THdfsParams;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class HdfsResourceTest {

    @Test
    public void testGenerateHdfsParamWithBdpAuthQueryId() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fs.defaultFS", "hdfs://localhost:9000");

        BDPAuthContext mockContext = Mockito.mock(BDPAuthContext.class);
        Mockito.when(mockContext.getErp()).thenReturn("test_user");
        Mockito.when(mockContext.getQueryId()).thenReturn(Mockito.mock(org.apache.doris.thrift.TUniqueId.class));
        Mockito.when(mockContext.getQueryIdStr()).thenReturn("test_query_id");

        try (MockedStatic<BDPAuthContext> mockedStatic = Mockito.mockStatic(BDPAuthContext.class)) {
            mockedStatic.when(BDPAuthContext::get).thenReturn(mockContext);

            THdfsParams result = HdfsResource.generateHdfsParam(properties);

            Assertions.assertNotNull(result);

            boolean hasBusinessId = result.getHdfsConf().stream()
                    .anyMatch(conf -> "BEE_BUSINESSID".equals(conf.getKey()) && "test_query_id".equals(conf.getValue()));
            Assertions.assertTrue(hasBusinessId);
        }
    }
}
