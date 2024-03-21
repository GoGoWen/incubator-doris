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

/* ******* Do not commit this file unless you know what you are doing ******* */

// **Note**: default db will be create if not exist
defaultDb = "regression_test"

jdbcUrl = "jdbc:mysql://127.0.0.1:9030/?useLocalSessionState=true&allowLoadLocalInfile=true"
targetJdbcUrl = "jdbc:mysql://127.0.0.1:9030/?useLocalSessionState=true&allowLoadLocalInfile=true"
jdbcUser = "root"
jdbcPassword = ""

ccrDownstreamUrl = "jdbc:mysql://172.19.0.2:9131/?useLocalSessionState=true&allowLoadLocalInfile=true"
ccrDownstreamUser = "root"
ccrDownstreamPassword = ""
ccrDownstreamFeThriftAddress = "127.0.0.1:9020"

feSourceThriftAddress = "127.0.0.1:9020"
feTargetThriftAddress = "127.0.0.1:9020"
feSyncerUser = "root"
feSyncerPassword = ""

feHttpAddress = "127.0.0.1:8030"
feHttpUser = "root"
feHttpPassword = ""

// set DORIS_HOME by system properties
// e.g. java -DDORIS_HOME=./
suitePath = "${DORIS_HOME}/regression-test/suites"
dataPath = "${DORIS_HOME}/regression-test/data"
pluginPath = "${DORIS_HOME}/regression-test/plugins"
realDataPath = "${DORIS_HOME}/regression-test/realdata"
// sf1DataPath can be url like "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com" or local path like "/data"
//sf1DataPath = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com"

// will test <group>/<suite>.groovy
// empty group will test all group
testGroups = ""
// empty suite will test all suite
testSuites = ""
// empty directories will test all directories
testDirectories = ""

// this groups will not be executed
excludeGroups = ""
// this suites will not be executed
excludeSuites = "test_information_schema_external,test_outfile_exception,test_digest,test_aggregate_all_functions2,test_hive_read_orc_complex_type,test_with_and_two_phase_agg,explode,test_cast_function,test_profile,test_broker_load_p2,test_spark_load,test_analyze_stats_p1,test_refresh_mtmv,test_bitmap_filter,test_export_parquet,test_doris_jdbc_catalog,,create_table_use_partition_policy,create_table_use_policy,doris_source,double_write_schema_change,flink_connector,load,load_four_step,load_one_step,load_three_step,load_two_step,modify_replica_use_partition,spark_connector,table_modify_resouce,test_big_kv_map,test_bitmap_index_load,test_bloom_filter_hit,test_build_index,test_build_index_fault,test_delete_where_in,test_point_query_load,test_scalar_types_load,test_scalar_types_load_p2,test_unique_mow_sequence,test_unique_mow_sequence"

// this directories will not be executed
excludeDirectories = "nereids_tpcds_shape_sf100_p0,nereids_tpch_shape_sf1000_p0,nereids_tpch_shape_sf500_p0,workload_manager_p1,fault_injection_p0"

customConf1 = "test_custom_conf_value"

s3Endpoint = "cos.ap-hongkong.myqcloud.com"
s3BucketName = "doris-build-hk-1308700295"
s3Region = "ap-hongkong"

max_failure_num=50

externalEnvIp="127.0.0.1"
