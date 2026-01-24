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

package org.apache.doris.mysql.privilege;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.authenticate.ldap.LdapManager;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class UserPropertyMgrTest {

    private UserPropertyMgr userPropertyMgr;
    @Mocked
    private Env env;
    @Mocked
    private Auth auth;
    @Mocked
    private LdapManager ldapManager;

    @Before
    public void setUp() {
        userPropertyMgr = new UserPropertyMgr();
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAuth();
                minTimes = 0;
                result = auth;

                auth.getLdapManager();
                minTimes = 0;
                result = ldapManager;

                ldapManager.doesUserExist(anyString);
                minTimes = 0;
                result = false;
            }
        };
    }

    @Test
    public void testGetEnableExternalFileCacheForNonExistentUser() {
        // Test 1: Non-existent user should return true (default value)
        String nonExistentUser = "non_existent_user";
        Assert.assertTrue("Default value for non-existent user should be true",
                userPropertyMgr.getEnableExternalFileCache(nonExistentUser));
    }

    @Test
    public void testGetEnableExternalFileCacheForUserWithoutProperty() throws UserException {
        // Test 2: User exists but property is not set, should return true (default value)
        String qualifiedUser = "testUser";
        userPropertyMgr.addUserResource(qualifiedUser);

        Assert.assertTrue("Default value for user without property should be true",
                userPropertyMgr.getEnableExternalFileCache(qualifiedUser));
    }

    @Test
    public void testGetEnableExternalFileCacheSetToFalse() throws UserException {
        // Test 3: User exists and property is set to false
        String qualifiedUser = "testUserFalse";
        userPropertyMgr.addUserResource(qualifiedUser);

        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.of("enable_external_file_cache", "false"));
        userPropertyMgr.updateUserProperty(qualifiedUser, properties, false);

        Assert.assertFalse("Should return false after setting property to false",
                userPropertyMgr.getEnableExternalFileCache(qualifiedUser));
    }

    @Test
    public void testGetEnableExternalFileCacheSetToTrue() throws UserException {
        // Test 4: User exists and property is set to true
        String qualifiedUser = "testUserTrue";
        userPropertyMgr.addUserResource(qualifiedUser);

        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.of("enable_external_file_cache", "true"));
        userPropertyMgr.updateUserProperty(qualifiedUser, properties, false);

        Assert.assertTrue("Should return true after setting property to true",
                userPropertyMgr.getEnableExternalFileCache(qualifiedUser));
    }

    @Test
    public void testGetEnableExternalFileCacheChangeValue() throws UserException {
        // Test 5: Change property value from false to true
        String qualifiedUser = "testUserChange";
        userPropertyMgr.addUserResource(qualifiedUser);

        // First set to false
        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.of("enable_external_file_cache", "false"));
        userPropertyMgr.updateUserProperty(qualifiedUser, properties, false);
        Assert.assertFalse("Should return false", userPropertyMgr.getEnableExternalFileCache(qualifiedUser));

        // Then change to true
        properties.clear();
        properties.add(Pair.of("enable_external_file_cache", "true"));
        userPropertyMgr.updateUserProperty(qualifiedUser, properties, false);
        Assert.assertTrue("Should return true after changing to true",
                userPropertyMgr.getEnableExternalFileCache(qualifiedUser));
    }

    @Test
    public void testGetEnableExternalFileCacheForLdapUser() {
        // Test 6: LDAP user should use LDAP_PROPERTY (default value is true)
        String ldapUser = "ldapUser";
        new Expectations() {
            {
                ldapManager.doesUserExist(ldapUser);
                minTimes = 1;
                result = true;
            }
        };

        // LDAP user doesn't exist in propertyMap, but getLdapPropertyIfNull should return LDAP_PROPERTY
        // LDAP_PROPERTY is created with default values, which should be true for enableExternalFileCache
        Assert.assertTrue("LDAP user should return default value (true)",
                userPropertyMgr.getEnableExternalFileCache(ldapUser));
    }

    @Test
    public void testGetEnableExternalFileCacheMultipleUsers() throws UserException {
        // Test 7: Multiple users with different property values
        String user1 = "user1";
        String user2 = "user2";
        String user3 = "user3";

        userPropertyMgr.addUserResource(user1);
        userPropertyMgr.addUserResource(user2);
        userPropertyMgr.addUserResource(user3);

        // user1: not set (default true)
        Assert.assertTrue("user1 should return default true", userPropertyMgr.getEnableExternalFileCache(user1));

        // user2: set to false
        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.of("enable_external_file_cache", "false"));
        userPropertyMgr.updateUserProperty(user2, properties, false);
        Assert.assertFalse("user2 should return false", userPropertyMgr.getEnableExternalFileCache(user2));

        // user3: set to true
        properties.clear();
        properties.add(Pair.of("enable_external_file_cache", "true"));
        userPropertyMgr.updateUserProperty(user3, properties, false);
        Assert.assertTrue("user3 should return true", userPropertyMgr.getEnableExternalFileCache(user3));

        // Verify user1 still returns true
        Assert.assertTrue("user1 should still return true", userPropertyMgr.getEnableExternalFileCache(user1));
    }
}
