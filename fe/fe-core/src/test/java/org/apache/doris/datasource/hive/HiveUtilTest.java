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

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class HiveUtilTest {
    @Test
    public void testGetHivePartitionValue() {
        String partitionValue1 = HiveUtil.getHivePartitionValue("a=c");
        Assertions.assertEquals("c", partitionValue1);
        String partitionValue2 = HiveUtil.getHivePartitionValue("a=c+1");
        Assertions.assertEquals("c+1", partitionValue2);
        String partitionValue3 = HiveUtil.getHivePartitionValue("a=d%2B1");
        Assertions.assertEquals("d+1", partitionValue3);
    }
}
