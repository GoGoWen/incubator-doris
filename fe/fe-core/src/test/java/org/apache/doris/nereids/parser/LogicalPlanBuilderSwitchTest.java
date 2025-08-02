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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.trees.plans.commands.use.SwitchCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for LogicalPlanBuilder's visitSwitchCatalog method through NereidsParser.
 */
public class LogicalPlanBuilderSwitchTest {

    private final NereidsParser parser = new NereidsParser();

    @Test
    public void testParseSwitchCatalogWithValidCatalog() {
        String sql = "SWITCH test_catalog";
        LogicalPlan plan = parser.parseSingle(sql);

        Assertions.assertTrue(plan instanceof SwitchCommand);
        SwitchCommand switchCommand = (SwitchCommand) plan;
        Assertions.assertEquals("SWITCH `test_catalog`", switchCommand.toSql());
    }

    @Test
    public void testParseSwitchCatalogWithQuotedCatalog() {
        String sql = "SWITCH `quoted_catalog`";
        LogicalPlan plan = parser.parseSingle(sql);

        Assertions.assertTrue(plan instanceof SwitchCommand);
        SwitchCommand switchCommand = (SwitchCommand) plan;
        Assertions.assertEquals("SWITCH `quoted_catalog`", switchCommand.toSql());
    }

    @Test
    public void testParseSwitchCatalogWithSpecialCharacters() {
        String sql = "SWITCH catalog_123";
        LogicalPlan plan = parser.parseSingle(sql);

        Assertions.assertTrue(plan instanceof SwitchCommand);
        SwitchCommand switchCommand = (SwitchCommand) plan;
        Assertions.assertEquals("SWITCH `catalog_123`", switchCommand.toSql());
    }

    @Test
    public void testParseSwitchCatalogWithMixedCase() {
        String sql = "SWITCH MyCatalog";
        LogicalPlan plan = parser.parseSingle(sql);

        Assertions.assertTrue(plan instanceof SwitchCommand);
        SwitchCommand switchCommand = (SwitchCommand) plan;
        Assertions.assertEquals("SWITCH `MyCatalog`", switchCommand.toSql());
    }

    @Test
    public void testParseSwitchCatalogCaseInsensitive() {
        String sql = "switch lowercase_catalog";
        LogicalPlan plan = parser.parseSingle(sql);

        Assertions.assertTrue(plan instanceof SwitchCommand);
        SwitchCommand switchCommand = (SwitchCommand) plan;
        Assertions.assertEquals("SWITCH `lowercase_catalog`", switchCommand.toSql());
    }

    @Test
    public void testParseSwitchWithoutCatalogName() {
        String sql = "SWITCH";

        Assertions.assertThrows(ParseException.class, () -> {
            parser.parseSingle(sql);
        });
    }
}
