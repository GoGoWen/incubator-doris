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

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class CommonUserPropertiesTest {

    @Test
    public void testGetEnableExternalFileCacheDefaultValue() {
        // Test 1: Default value should be true
        CommonUserProperties properties = new CommonUserProperties();
        Assert.assertTrue("Default value of enableExternalFileCache should be true",
                properties.getEnableExternalFileCache());
    }

    @Test
    public void testSetAndGetEnableExternalFileCacheTrue() {
        // Test 2: Set to true and get
        CommonUserProperties properties = new CommonUserProperties();
        properties.setEnableExternalFileCache(true);
        Assert.assertTrue("Should return true after setting to true",
                properties.getEnableExternalFileCache());
    }

    @Test
    public void testSetAndGetEnableExternalFileCacheFalse() {
        // Test 3: Set to false and get
        CommonUserProperties properties = new CommonUserProperties();
        properties.setEnableExternalFileCache(false);
        Assert.assertFalse("Should return false after setting to false",
                properties.getEnableExternalFileCache());
    }

    @Test
    public void testSetAndGetEnableExternalFileCacheMultipleChanges() {
        // Test 4: Multiple changes
        CommonUserProperties properties = new CommonUserProperties();

        // Initially true (default)
        Assert.assertTrue("Initial value should be true", properties.getEnableExternalFileCache());

        // Change to false
        properties.setEnableExternalFileCache(false);
        Assert.assertFalse("Should be false after setting to false",
                properties.getEnableExternalFileCache());

        // Change back to true
        properties.setEnableExternalFileCache(true);
        Assert.assertTrue("Should be true after setting to true",
                properties.getEnableExternalFileCache());

        // Change to false again
        properties.setEnableExternalFileCache(false);
        Assert.assertFalse("Should be false after setting to false again",
                properties.getEnableExternalFileCache());
    }

    @Test
    public void testSetEnableExternalFileCacheWithSameValue() {
        // Test 5: Set to the same value multiple times
        CommonUserProperties properties = new CommonUserProperties();

        // Set to true multiple times
        properties.setEnableExternalFileCache(true);
        properties.setEnableExternalFileCache(true);
        properties.setEnableExternalFileCache(true);
        Assert.assertTrue("Should remain true after multiple sets to true",
                properties.getEnableExternalFileCache());

        // Set to false multiple times
        properties.setEnableExternalFileCache(false);
        properties.setEnableExternalFileCache(false);
        properties.setEnableExternalFileCache(false);
        Assert.assertFalse("Should remain false after multiple sets to false",
                properties.getEnableExternalFileCache());
    }

    @Test
    public void testEnableExternalFileCacheSerialization() throws IOException {
        // Test 6: Serialization and deserialization
        CommonUserProperties original = new CommonUserProperties();
        original.setEnableExternalFileCache(false);

        // Serialize
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(byteStream);
        original.write(outputStream);
        outputStream.flush();

        // Deserialize
        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray()));
        CommonUserProperties deserialized = CommonUserProperties.read(inputStream);

        // Verify the value is preserved
        Assert.assertEquals("Serialized and deserialized value should match",
                original.getEnableExternalFileCache(), deserialized.getEnableExternalFileCache());
        Assert.assertFalse("Deserialized value should be false",
                deserialized.getEnableExternalFileCache());
    }

    @Test
    public void testEnableExternalFileCacheSerializationTrue() throws IOException {
        // Test 7: Serialization with true value
        CommonUserProperties original = new CommonUserProperties();
        original.setEnableExternalFileCache(true);

        // Serialize
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(byteStream);
        original.write(outputStream);
        outputStream.flush();

        // Deserialize
        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray()));
        CommonUserProperties deserialized = CommonUserProperties.read(inputStream);

        // Verify the value is preserved
        Assert.assertEquals("Serialized and deserialized value should match",
                original.getEnableExternalFileCache(), deserialized.getEnableExternalFileCache());
        Assert.assertTrue("Deserialized value should be true",
                deserialized.getEnableExternalFileCache());
    }

    @Test
    public void testEnableExternalFileCacheSerializationDefaultValue() throws IOException {
        // Test 8: Serialization with default value (true)
        CommonUserProperties original = new CommonUserProperties();
        // Don't set, use default value

        // Serialize
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(byteStream);
        original.write(outputStream);
        outputStream.flush();

        // Deserialize
        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray()));
        CommonUserProperties deserialized = CommonUserProperties.read(inputStream);

        // Verify the default value is preserved
        Assert.assertTrue("Deserialized default value should be true",
                deserialized.getEnableExternalFileCache());
    }

    @Test
    public void testEnableExternalFileCacheIndependence() {
        // Test 9: Multiple instances are independent
        CommonUserProperties properties1 = new CommonUserProperties();
        CommonUserProperties properties2 = new CommonUserProperties();

        // Set different values
        properties1.setEnableExternalFileCache(true);
        properties2.setEnableExternalFileCache(false);

        // Verify they are independent
        Assert.assertTrue("properties1 should be true", properties1.getEnableExternalFileCache());
        Assert.assertFalse("properties2 should be false", properties2.getEnableExternalFileCache());

        // Change one, verify the other is not affected
        properties1.setEnableExternalFileCache(false);
        Assert.assertFalse("properties1 should now be false", properties1.getEnableExternalFileCache());
        Assert.assertFalse("properties2 should still be false", properties2.getEnableExternalFileCache());
    }
}
