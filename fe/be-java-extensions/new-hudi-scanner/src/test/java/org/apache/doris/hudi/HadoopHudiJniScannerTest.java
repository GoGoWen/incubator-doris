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

package org.apache.doris.hudi;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Map;

public class HadoopHudiJniScannerTest {

    @Test
    public void testHadoopHudiJniScannerConstructor() {
        Map<String, String> params = Maps.newHashMap();
        params.put("base_path", "hdfs://ns0001:9000/hudi/test");
        params.put("data_file_path", "hdfs://ns0001:9000/hudi/test/file_path");
        params.put("data_file_length", "1024");
        params.put("instant_time", "1672531200");
        params.put("serde", "org.apache.hadoop.hive.serde2.OpenCSVSerde");
        params.put("input_format", "org.apache.hadoop.hive.ql.io.HiveInputFormat");
        params.put("hudi_column_names", "id,name,age");
        params.put("hudi_column_types", "int#string#int");
        params.put("hudi_primary_keys", "id");
        params.put("hadoop_conf.test", "test");
        params.put("HADOOP_USER_NAME", "test");
        params.put("HADOOP_USER_TOKEN", "xxxxxxx");
        HadoopHudiJniScanner scanner = new HadoopHudiJniScanner(1024, params);
        Assertions.assertEquals("hdfs://ns0001:9000/hudi/test", scanner.getBasePath());
        Assertions.assertEquals("hdfs://ns0001:9000/hudi/test/file_path", scanner.getDataFilePath());
        Assertions.assertEquals(1024, scanner.getDataFileLength());
        Assertions.assertEquals(0, scanner.getDeltaFilePaths().length);
        Assertions.assertEquals("1672531200", scanner.getInstantTime());
        Assertions.assertEquals("org.apache.hadoop.hive.serde2.OpenCSVSerde", scanner.getSerde());
        Assertions.assertEquals("org.apache.hadoop.hive.ql.io.HiveInputFormat", scanner.getInputFormat());
        Assertions.assertEquals("id,name,age", scanner.getHudiColumnNames());
        Assertions.assertEquals(new String[]{"int", "string", "int"}.length, scanner.getHudiColumnTypes().length);
        Assertions.assertEquals(new String[]{"id"}[0], scanner.getRequiredFields()[0]);
        Assertions.assertEquals("test", scanner.getFsOptionsProps().get("test"));
        Assertions.assertEquals(ZoneId.systemDefault(), scanner.getColumnValue().getZoneId());
        Assertions.assertEquals(1024, scanner.getFetchSize());
        Assertions.assertEquals("test", scanner.getHadoopUserName());
        Assertions.assertEquals("xxxxxxx", scanner.getHadoopUserToken());
        params.put("delta_file_paths", "hdfs://ns0001:9000/hudi/test/delta_file_paths");
        params.put("required_fields", "id,name");
        params.put("time_zone", "Asia/Shanghai");
        HadoopHudiJniScanner scanner2 = new HadoopHudiJniScanner(1024, params);
        Assertions.assertEquals(new String[]{"hdfs://ns0001:9000/hudi/test/delta_file_paths"}[0],
                scanner2.getDeltaFilePaths()[0]);
        Assertions.assertEquals(new String[]{"id", "name"}[0], scanner2.getRequiredFields()[0]);
        Assertions.assertEquals(ZoneId.of("Asia/Shanghai"), scanner2.getColumnValue().getZoneId());
    }
}
