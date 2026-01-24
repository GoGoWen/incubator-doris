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

import org.apache.doris.blockrule.SqlBlockRuleMgr;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.load.DppConfig;
import org.apache.doris.mysql.privilege.UserProperty;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class UserPropertyTest {
    private FakeEnv fakeEnv;
    @Mocked
    private Env env;
    @Mocked
    private SqlBlockRuleMgr sqlBlockRuleMgr;

    @Before
    public void setUp() {
        // Use FakeEnv to mock static methods
        fakeEnv = new FakeEnv();
        FakeEnv.setMetaVersion(FeConstants.meta_version);
        FakeEnv.setEnv(env);

        new Expectations() {
            {
                env.getSqlBlockRuleMgr();
                minTimes = 0;
                result = sqlBlockRuleMgr;

                sqlBlockRuleMgr.existRule("rule1");
                minTimes = 0;
                result = true;

                sqlBlockRuleMgr.existRule("rule2");
                minTimes = 0;
                result = true;

                sqlBlockRuleMgr.existRule("test1");
                minTimes = 0;
                result = true;

                sqlBlockRuleMgr.existRule("test2");
                minTimes = 0;
                result = true;

                sqlBlockRuleMgr.existRule("test3");
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testNormal() throws IOException, DdlException {
        String qualifiedUser = "root";
        UserProperty property = new UserProperty(qualifiedUser);
        // To image
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(byteStream);
        property.write(outputStream);
        outputStream.flush();

        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray()));
        UserProperty newProperty = UserProperty.read(inputStream);
        Assert.assertEquals(qualifiedUser, newProperty.getQualifiedUser());
    }

    @Test
    public void testUpdate() throws UserException {
        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.of("MAX_USER_CONNECTIONS", "100"));
        properties.add(Pair.of("load_cluster.dpp-cluster.hadoop_palo_path", "/user/palo2"));
        properties.add(Pair.of("default_load_cluster", "dpp-cluster"));
        properties.add(Pair.of("max_qUERY_instances", "3000"));
        properties.add(Pair.of("parallel_fragment_exec_instance_num", "2000"));
        properties.add(Pair.of("sql_block_rules", "rule1,rule2"));
        properties.add(Pair.of("cpu_resource_limit", "2"));
        properties.add(Pair.of("query_timeout", "500"));
        properties.add(Pair.of("exec_mem_limit", "2147483648"));

        UserProperty userProperty = new UserProperty();
        userProperty.update(properties);
        Assert.assertEquals(100, userProperty.getMaxConn());
        Assert.assertEquals("/user/palo2", userProperty.getLoadClusterInfo("dpp-cluster").second.getPaloPath());
        Assert.assertEquals("dpp-cluster", userProperty.getDefaultLoadCluster());
        Assert.assertEquals(3000, userProperty.getMaxQueryInstances());
        Assert.assertEquals(2000, userProperty.getParallelFragmentExecInstanceNum());
        Assert.assertEquals(new String[]{"rule1", "rule2"}, userProperty.getSqlBlockRules());
        Assert.assertEquals(2, userProperty.getCpuResourceLimit());
        Assert.assertEquals(500, userProperty.getQueryTimeout());
        Assert.assertEquals(Sets.newHashSet(), userProperty.getCopiedResourceTags());
        Assert.assertEquals(2147483648L, userProperty.getExecMemLimit());

        // fetch property
        List<List<String>> rows = userProperty.fetchProperty();
        for (List<String> row : rows) {
            String key = row.get(0);
            String value = row.get(1);

            if (key.equalsIgnoreCase("max_user_connections")) {
                Assert.assertEquals("100", value);
            } else if (key.equalsIgnoreCase("load_cluster.dpp-cluster.hadoop_palo_path")) {
                Assert.assertEquals("/user/palo2", value);
            } else if (key.equalsIgnoreCase("default_load_cluster")) {
                Assert.assertEquals("dpp-cluster", value);
            } else if (key.equalsIgnoreCase("max_query_instances")) {
                Assert.assertEquals("3000", value);
            } else if (key.equalsIgnoreCase("sql_block_rules")) {
                Assert.assertEquals("rule1,rule2", value);
            } else if (key.equalsIgnoreCase("cpu_resource_limit")) {
                Assert.assertEquals("2", value);
            } else if (key.equalsIgnoreCase("query_timeout")) {
                Assert.assertEquals("500", value);
            } else if (key.equalsIgnoreCase("exec_mem_limit")) {
                Assert.assertEquals("2147483648", value);
            }
        }

        // get cluster info
        DppConfig dppConfig = userProperty.getLoadClusterInfo("dpp-cluster").second;
        Assert.assertEquals(8070, dppConfig.getHttpPort());

        // set palo path null
        properties.clear();
        properties.add(Pair.of("load_cluster.dpp-cluster.hadoop_palo_path", null));
        userProperty.update(properties);
        Assert.assertEquals(null, userProperty.getLoadClusterInfo("dpp-cluster").second.getPaloPath());

        // remove dpp-cluster
        properties.clear();
        properties.add(Pair.of("load_cluster.dpp-cluster", null));
        Assert.assertEquals("dpp-cluster", userProperty.getDefaultLoadCluster());
        userProperty.update(properties);
        Assert.assertEquals(null, userProperty.getLoadClusterInfo("dpp-cluster").second);
        Assert.assertEquals(null, userProperty.getDefaultLoadCluster());

        // sql block rule
        properties.clear();
        properties.add(Pair.of("sql_block_rules", ""));
        userProperty.update(properties);
        Assert.assertEquals(1, userProperty.getSqlBlockRules().length);
        properties.clear();
        properties.add(Pair.of("sql_block_rules", "test1, test2,test3"));
        userProperty.update(properties);
        Assert.assertEquals(3, userProperty.getSqlBlockRules().length);
    }

    @Test
    public void testValidation() throws UserException {
        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.of("cpu_resource_limit", "-1"));
        UserProperty userProperty = new UserProperty();
        userProperty.update(properties);
        Assert.assertEquals(-1, userProperty.getCpuResourceLimit());

        properties = Lists.newArrayList();
        properties.add(Pair.of("cpu_resource_limit", "-2"));
        userProperty = new UserProperty();
        try {
            userProperty.update(properties);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("is not valid"));
        }
        Assert.assertEquals(-1, userProperty.getCpuResourceLimit());
    }

    @Test
    public void testGetEnableExternalFileCache() {
        // Test 1: Default value should be true
        UserProperty userProperty = new UserProperty();
        Assert.assertTrue("Default value of enableExternalFileCache should be true",
                userProperty.getEnableExternalFileCache());
    }

    @Test
    public void testUpdateEnableExternalFileCache() throws UserException {
        // Test 2: Update enable_external_file_cache to true
        UserProperty userProperty = new UserProperty();
        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.of("enable_external_file_cache", "true"));
        userProperty.update(properties);
        Assert.assertTrue("Should return true after setting to true",
                userProperty.getEnableExternalFileCache());

        // Test 3: Update enable_external_file_cache to false
        properties.clear();
        properties.add(Pair.of("enable_external_file_cache", "false"));
        userProperty.update(properties);
        Assert.assertFalse("Should return false after setting to false",
                userProperty.getEnableExternalFileCache());

        // Test 4: Update enable_external_file_cache back to true
        properties.clear();
        properties.add(Pair.of("enable_external_file_cache", "true"));
        userProperty.update(properties);
        Assert.assertTrue("Should return true after setting back to true",
                userProperty.getEnableExternalFileCache());
    }

    @Test
    public void testUpdateEnableExternalFileCacheCaseInsensitive() throws UserException {
        // Test 5: Case insensitive property name
        UserProperty userProperty = new UserProperty();
        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.of("ENABLE_EXTERNAL_FILE_CACHE", "false"));
        userProperty.update(properties);
        Assert.assertFalse("Should handle case insensitive property name",
                userProperty.getEnableExternalFileCache());

        properties.clear();
        properties.add(Pair.of("Enable_External_File_Cache", "true"));
        userProperty.update(properties);
        Assert.assertTrue("Should handle mixed case property name",
                userProperty.getEnableExternalFileCache());
    }

    @Test
    public void testUpdateEnableExternalFileCacheFormatError() {
        // Test 6: Format error - key with dot separator
        UserProperty userProperty = new UserProperty();
        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.of("enable_external_file_cache.subkey", "true"));
        try {
            userProperty.update(properties);
            Assert.fail("Should throw DdlException for format error");
        } catch (UserException e) {
            Assert.assertTrue("Should throw format error",
                    e.getMessage().contains("format error"));
        }
    }

    @Test
    public void testUpdateEnableExternalFileCacheInvalidBoolean() throws UserException {
        // Test 7: Invalid boolean value - Boolean.parseBoolean returns false for invalid strings
        // Note: Boolean.parseBoolean doesn't throw exception, it just returns false for non-"true" strings
        UserProperty userProperty = new UserProperty();
        List<Pair<String, String>> properties = Lists.newArrayList();

        // "invalid" will be parsed as false by Boolean.parseBoolean (it never throws exception)
        properties.add(Pair.of("enable_external_file_cache", "invalid"));
        userProperty.update(properties);
        // Boolean.parseBoolean doesn't throw exception, so this should succeed
        // but the value will be false (any non-"true" string is parsed as false)
        Assert.assertFalse("Invalid string should be parsed as false",
                userProperty.getEnableExternalFileCache());

        // Test with empty string
        properties.clear();
        properties.add(Pair.of("enable_external_file_cache", ""));
        userProperty.update(properties);
        Assert.assertFalse("Empty string should be parsed as false",
                userProperty.getEnableExternalFileCache());

        // Test with "1" (not "true")
        properties.clear();
        properties.add(Pair.of("enable_external_file_cache", "1"));
        userProperty.update(properties);
        Assert.assertFalse("'1' should be parsed as false (only 'true' is true)",
                userProperty.getEnableExternalFileCache());
    }

    @Test
    public void testUpdateEnableExternalFileCacheInFetchProperty() throws UserException {
        // Test 8: Verify enable_external_file_cache appears in fetchProperty
        UserProperty userProperty = new UserProperty();
        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.of("enable_external_file_cache", "false"));
        userProperty.update(properties);

        List<List<String>> rows = userProperty.fetchProperty();
        boolean found = false;
        for (List<String> row : rows) {
            String key = row.get(0);
            String value = row.get(1);
            if (key.equalsIgnoreCase("enable_external_file_cache")) {
                found = true;
                Assert.assertEquals("false", value);
                break;
            }
        }
        Assert.assertTrue("enable_external_file_cache should appear in fetchProperty", found);
    }

    @Test
    public void testUpdateEnableExternalFileCacheWithOtherProperties() throws UserException {
        // Test 9: Update enable_external_file_cache along with other properties
        UserProperty userProperty = new UserProperty();
        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.of("max_user_connections", "200"));
        properties.add(Pair.of("enable_external_file_cache", "false"));
        properties.add(Pair.of("query_timeout", "300"));
        userProperty.update(properties);

        Assert.assertEquals(200, userProperty.getMaxConn());
        Assert.assertFalse("enable_external_file_cache should be false",
                userProperty.getEnableExternalFileCache());
        Assert.assertEquals(300, userProperty.getQueryTimeout());
    }

    @Test
    public void testUpdateEnableExternalFileCacheDefaultValueInFetchProperty() {
        // Test 10: Verify default value (true) appears in fetchProperty
        UserProperty userProperty = new UserProperty();
        List<List<String>> rows = userProperty.fetchProperty();
        boolean found = false;
        for (List<String> row : rows) {
            String key = row.get(0);
            String value = row.get(1);
            if (key.equalsIgnoreCase("enable_external_file_cache")) {
                found = true;
                Assert.assertEquals("true", value);
                break;
            }
        }
        Assert.assertTrue("enable_external_file_cache should appear in fetchProperty with default value", found);
    }
}
