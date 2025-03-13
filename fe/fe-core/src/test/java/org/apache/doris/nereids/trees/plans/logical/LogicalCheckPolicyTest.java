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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.es.EsExternalCatalog;
import org.apache.doris.datasource.es.EsExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.RowFilterPolicy;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy.RelatedPolicy;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class LogicalCheckPolicyTest {
    @Test
    public void testFindNoPloicy(@Injectable CascadesContext cascadesContext,
            @Injectable ConnectContext connectContext, @Injectable Env env,
            @Injectable AccessControllerManager accessManager,
            @Injectable LogicalCatalogRelation catalogRelation) {
        LogicalEmptyRelation logicalRelation = new LogicalEmptyRelation(new RelationId(1), new ArrayList());
        LogicalCheckPolicy<LogicalEmptyRelation> logicalCheckPolicy = new LogicalCheckPolicy<>(logicalRelation);
        RelatedPolicy relatedPolicy = logicalCheckPolicy.findPolicy(logicalRelation, cascadesContext);
        Assertions.assertEquals(RelatedPolicy.NO_POLICY, relatedPolicy);

        new Expectations() {
            {
                cascadesContext.getConnectContext();
                result = connectContext;

                connectContext.getEnv();
                result = env;

                env.getAccessManager();
                result = accessManager;

                connectContext.getCurrentUserIdentity();
                result = new UserIdentity("root", "127.0.0.1");

                catalogRelation.getDatabase();
                result = new EsExternalDatabase(new EsExternalCatalog(1, "es", "",
                        Maps.newHashMap(), "test"), 2, "es");
            }
        };

        LogicalCheckPolicy<LogicalCatalogRelation> catalogRelationLogicalCheckPolicy =
                new LogicalCheckPolicy<>(catalogRelation);
        relatedPolicy = catalogRelationLogicalCheckPolicy.findPolicy(catalogRelation, cascadesContext);
        Assertions.assertEquals(RelatedPolicy.NO_POLICY, relatedPolicy);

        new Expectations() {
            {
                connectContext.getCurrentUserIdentity();
                result = new UserIdentity("admin", "127.0.0.1");
            }
        };
        relatedPolicy = catalogRelationLogicalCheckPolicy.findPolicy(catalogRelation, cascadesContext);
        Assertions.assertEquals(RelatedPolicy.NO_POLICY, relatedPolicy);
    }

    @Test
    public void testGetRowFilterPolicies(@Injectable LogicalCatalogRelation catalogRelation,
            @Injectable AccessControllerManager accessManager,
            @Injectable HMSExternalTable hmsExternalTable) {
        LogicalCheckPolicy<LogicalCatalogRelation> catalogRelationLogicalCheckPolicy =
                new LogicalCheckPolicy<>(catalogRelation);
        UserIdentity currentUserIdentity = new UserIdentity("test", "127.0.0.1");
        Expression expression = new NereidsParser().parseExpression("(id < 1)");
        new Expectations() {
            {
                accessManager.evalRowFilterPolicies(currentUserIdentity, "test", "test", "test");
                result = new ArrayList();
                catalogRelation.getTable();
                result = hmsExternalTable;
                hmsExternalTable.getRowPolicy();
                result = expression;
            }
        };
        List<? extends RowFilterPolicy> policies = catalogRelationLogicalCheckPolicy.getRowFilterPolicies(
                catalogRelation, currentUserIdentity, accessManager, "test", "test", "test");
        Assertions.assertEquals(1, policies.size());
        try {
            Assertions.assertEquals(expression, policies.get(0).getFilterExpression());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
        Assertions.assertEquals("custom policy: (id < 1)", policies.get(0).getPolicyIdent());
    }
}
