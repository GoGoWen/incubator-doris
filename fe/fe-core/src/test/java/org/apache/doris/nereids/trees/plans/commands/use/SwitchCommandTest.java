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

package org.apache.doris.nereids.trees.plans.commands.use;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for SwitchCommand.
 */
public class SwitchCommandTest {
    @Injectable
    private ConnectContext connectContext;

    @Injectable
    private StmtExecutor stmtExecutor;

    @Mocked
    private Env env;

    @Mocked
    private AccessControllerManager accessManager;

    private SwitchCommand switchCommand;

    @BeforeEach
    public void setUp() {
        switchCommand = new SwitchCommand("test_catalog");
    }

    @Test
    public void testToSql() {
        SwitchCommand cmd = new SwitchCommand("test_catalog");
        Assertions.assertEquals("SWITCH `test_catalog`", cmd.toSql());
    }

    @Test
    public void testToSqlWithSpecialCharacters() {
        SwitchCommand cmd = new SwitchCommand("catalog_123");
        Assertions.assertEquals("SWITCH `catalog_123`", cmd.toSql());
    }

    @Test
    public void testToSqlWithMixedCase() {
        SwitchCommand cmd = new SwitchCommand("MyCatalog");
        Assertions.assertEquals("SWITCH `MyCatalog`", cmd.toSql());
    }

    @Test
    public void testToSqlWithEmptyString() {
        SwitchCommand cmd = new SwitchCommand("");
        Assertions.assertEquals("SWITCH ``", cmd.toSql());
    }

    @Test
    public void testAccept() {
        SwitchCommand switchCommand = new SwitchCommand("test_catalog");
        TestCommandVisitor visitor = new TestCommandVisitor();
        String result = switchCommand.accept(visitor, "test_context");
        Assertions.assertEquals("visitSwitchCommand_test_context", result);
    }

    @Test
    public void testRunSuccess() throws Exception {
        // Mock static methods and environment
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return env;
            }
        };

        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                return connectContext;
            }
        };

        new MockUp<Util>() {
            @Mock
            public void checkCatalogAllRules(String catalogName) {
                // Do nothing - validation passes
            }
        };

        QueryState queryState = new QueryState();

        new Expectations() {
            {
                env.getAccessManager();
                result = accessManager;

                accessManager.checkCtlPriv(connectContext, "test_catalog", PrivPredicate.SHOW);
                result = true;

                connectContext.getEnv();
                result = env;

                env.changeCatalog(connectContext, "test_catalog");
                // No exception thrown - success case

                connectContext.getState();
                result = queryState;
            }
        };

        // Execute the run method
        switchCommand.run(connectContext, stmtExecutor);

        // Verify success state
        Assertions.assertEquals(QueryState.MysqlStateType.OK, queryState.getStateType());
    }

    @Test
    public void testRunWithAccessDenied() throws Exception {
        // Mock static methods and environment
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return env;
            }
        };

        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                return connectContext;
            }
        };

        new MockUp<Util>() {
            @Mock
            public void checkCatalogAllRules(String catalogName) {
                // Do nothing - validation passes
            }
        };

        new MockUp<ErrorReport>() {
            @Mock
            public void reportAnalysisException(org.apache.doris.common.ErrorCode errorCode, Object... args)
                    throws AnalysisException {
                throw new AnalysisException("Access denied for user '" + args[0] + "' to catalog '" + args[1] + "'");
            }
        };

        new Expectations() {
            {
                env.getAccessManager();
                result = accessManager;

                accessManager.checkCtlPriv(connectContext, "test_catalog", PrivPredicate.SHOW);
                result = false; // Access denied

                connectContext.getQualifiedUser();
                result = "test_user";
            }
        };

        // Execute and expect AnalysisException
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            switchCommand.run(connectContext, stmtExecutor);
        });

        Assertions.assertTrue(exception.getMessage().contains("Access denied"));
    }

    @Test
    public void testRunWithInvalidCatalogName() throws Exception {
        SwitchCommand invalidCommand = new SwitchCommand("invalid@catalog");

        new MockUp<Util>() {
            @Mock
            public void checkCatalogAllRules(String catalogName) throws AnalysisException {
                throw new AnalysisException("Invalid catalog name: " + catalogName);
            }
        };

        // Execute and expect AnalysisException for invalid catalog name
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            invalidCommand.run(connectContext, stmtExecutor);
        });

        Assertions.assertTrue(exception.getMessage().contains("Invalid catalog name"));
    }

    @Test
    public void testRunWithDdlException() throws Exception {
        // Mock static methods and environment
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return env;
            }
        };

        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                return connectContext;
            }
        };

        new MockUp<Util>() {
            @Mock
            public void checkCatalogAllRules(String catalogName) {
                // Do nothing - validation passes
            }
        };

        QueryState queryState = new QueryState();

        new Expectations() {
            {
                env.getAccessManager();
                result = accessManager;

                accessManager.checkCtlPriv(connectContext, "test_catalog", PrivPredicate.SHOW);
                result = true;

                connectContext.getEnv();
                result = env;

                env.changeCatalog(connectContext, "test_catalog");
                result = new DdlException("Catalog not found: test_catalog"); // Throw DdlException

                connectContext.getState();
                result = queryState;
            }
        };

        // Execute the run method
        switchCommand.run(connectContext, stmtExecutor);

        // Verify error state is set
        Assertions.assertEquals(queryState.getStateType(), QueryState.MysqlStateType.ERR);
    }

    @Test
    public void testRunWithNullCatalogName() throws Exception {
        SwitchCommand nullCommand = new SwitchCommand(null);

        new MockUp<Util>() {
            @Mock
            public void checkCatalogAllRules(String catalogName) throws AnalysisException {
                if (catalogName == null) {
                    throw new AnalysisException("Catalog name cannot be null");
                }
            }
        };

        // Execute and expect AnalysisException for null catalog name
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            nullCommand.run(connectContext, stmtExecutor);
        });

        Assertions.assertTrue(exception.getMessage().contains("Catalog name cannot be null"));
    }

    /**
     * Test visitor implementation for testing accept method.
     */
    private static class TestCommandVisitor extends PlanVisitor<String, String> {
        @Override
        public String visitSwitchCommand(SwitchCommand switchCommand, String context) {
            return "visitSwitchCommand_" + context;
        }

        @Override
        public String visit(Plan plan, String context) {
            return "";
        }

        @Override
        public String visitCommand(org.apache.doris.nereids.trees.plans.commands.Command command, String context) {
            return "visitCommand_" + context;
        }
    }
}
