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

package org.apache.doris.common.util;

import org.junit.Assert;
import org.junit.Test;

public class HiveViewSqlTransformerTest {

    @Test
    public void testFormat() {
        String sql1 = "select 'TEST1'";
        Assert.assertEquals(sql1, HiveViewSqlTransformer.format(sql1));
        String sql2 = "SELECT col1, col2, col3 from test_db.TEST_TABLE where COL1 = \"TEST\"";
        Assert.assertEquals("select col1, col2, col3 from test_db.test_table where col1 = \"TEST\"",
                HiveViewSqlTransformer.format(sql2));
        String sql3 = "SELECT count(col1), SUM(col2), col3, col4 from TEST_DB.TEST_TABLE GROUP BY COL3, col4 "
                + "where COL3 = \"TEST\" and COL4='YES'";
        Assert.assertEquals("select count(col1), sum(col2), col3, col4 from test_db.test_table group by "
                + "col3, col4 where col3 = \"TEST\" and col4='YES'", HiveViewSqlTransformer.format(sql3));
        String sql4 = "SELECT COL1, COL2 from TEST_DB.TEST_TABLE where COL1 = 'TEST' and COL2='YES'";
        Assert.assertEquals("select col1, col2 from test_db.test_table where col1 = 'TEST' and col2='YES'",
                HiveViewSqlTransformer.format(sql4));
        String sql5 = "SELECT COL1, COL2 from TEST_DB.TEST_TABLE where COL1 = \"TEST\" and COL2=\"YES\"";
        Assert.assertEquals("select col1, col2 from test_db.test_table where col1 = \"TEST\" and col2=\"YES\"",
                HiveViewSqlTransformer.format(sql5));
        String sql6 = "SELECT COL1, COL2 from TEST_DB.TEST_TABLE where COL1 = 'TEST' and COL2=\"YES\"";
        Assert.assertEquals("select col1, col2 from test_db.test_table where col1 = 'TEST' and col2=\"YES\"",
                HiveViewSqlTransformer.format(sql6));
    }
}
