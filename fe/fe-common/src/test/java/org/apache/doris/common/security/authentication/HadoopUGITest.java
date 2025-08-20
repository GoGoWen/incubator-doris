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

package org.apache.doris.common.security.authentication;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class HadoopUGITest {

    /**
     * Test ugiDoAs with an exception that has no cause.
     * This tests the changed code path where e.getCause() == null, so the original exception is used.
     */
    @Test
    public void testUgiDoAs_ExceptionWithoutCause() {
        String expectedMessage = "Direct exception message";

        // Create an action that throws an exception without a cause
        PrivilegedExceptionAction<String> action = new PrivilegedExceptionAction<String>() {
            @Override
            public String run() throws Exception {
                throw new RuntimeException(expectedMessage);
            }
        };

        try {
            // Call ugiDoAs with null auth config (will execute action.run() directly)
            HadoopUGI.ugiDoAs(null, action);
            Assert.fail("Should have thrown RuntimeException");
        } catch (RuntimeException e) {
            // Verify the exception handling logic:
            // When cause is null, the original exception's message and the exception itself are used
            Assert.assertEquals(expectedMessage, e.getMessage());
            Assert.assertNotNull(e.getCause());
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
            Assert.assertEquals(expectedMessage, e.getCause().getMessage());
        }
    }

    /**
     * Test ugiDoAs with an exception that has a cause.
     * This tests the changed code path where e.getCause() != null, so the cause is extracted.
     */
    @Test
    public void testUgiDoAs_ExceptionWithCause() {
        String causeMessage = "Root cause message";
        String wrapperMessage = "Wrapper exception message";

        // Create an action that throws an exception with a cause
        PrivilegedExceptionAction<String> action = new PrivilegedExceptionAction<String>() {
            @Override
            public String run() throws Exception {
                IOException cause = new IOException(causeMessage);
                throw new Exception(wrapperMessage, cause);
            }
        };

        try {
            // Call ugiDoAs with null auth config (will execute action.run() directly)
            HadoopUGI.ugiDoAs(null, action);
            Assert.fail("Should have thrown RuntimeException");
        } catch (RuntimeException e) {
            // Verify the exception handling logic:
            // When cause is present, the cause's message and the cause itself are used
            Assert.assertEquals(causeMessage, e.getMessage());
            Assert.assertNotNull(e.getCause());
            Assert.assertTrue(e.getCause() instanceof IOException);
            Assert.assertEquals(causeMessage, e.getCause().getMessage());
        }
    }

    /**
     * Test ugiDoAs with nested exception (exception with cause that has its own cause).
     * This verifies that only the first-level cause is extracted, not nested causes.
     */
    @Test
    public void testUgiDoAs_NestedExceptionWithCause() {
        String deepCauseMessage = "Deep root cause";
        String causeMessage = "Intermediate cause";
        String wrapperMessage = "Top level exception";

        // Create an action that throws a nested exception
        PrivilegedExceptionAction<String> action = new PrivilegedExceptionAction<String>() {
            @Override
            public String run() throws Exception {
                IllegalArgumentException deepCause = new IllegalArgumentException(deepCauseMessage);
                IOException intermediateCause = new IOException(causeMessage, deepCause);
                throw new Exception(wrapperMessage, intermediateCause);
            }
        };

        try {
            // Call ugiDoAs with null auth config (will execute action.run() directly)
            HadoopUGI.ugiDoAs(null, action);
            Assert.fail("Should have thrown RuntimeException");
        } catch (RuntimeException e) {
            // Verify only the first-level cause is extracted
            Assert.assertEquals(causeMessage, e.getMessage());
            Assert.assertNotNull(e.getCause());
            Assert.assertTrue(e.getCause() instanceof IOException);
            Assert.assertEquals(causeMessage, e.getCause().getMessage());
            // The IOException itself has a cause (the deep cause)
            Assert.assertNotNull(e.getCause().getCause());
            Assert.assertTrue(e.getCause().getCause() instanceof IllegalArgumentException);
        }
    }

    /**
     * Test ugiDoAs with successful execution (no exception).
     * This ensures the method works correctly when no exception is thrown.
     */
    @Test
    public void testUgiDoAs_SuccessfulExecution() {
        String expectedResult = "Success";

        // Create an action that succeeds
        PrivilegedExceptionAction<String> action = new PrivilegedExceptionAction<String>() {
            @Override
            public String run() throws Exception {
                return expectedResult;
            }
        };

        // Call ugiDoAs with null auth config (will execute action.run() directly)
        String result = HadoopUGI.ugiDoAs(null, action);

        Assert.assertEquals(expectedResult, result);
    }
}
