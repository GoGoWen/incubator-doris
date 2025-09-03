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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.tablefunction.PartitionValuesTableValuedFunction.PartitionAggOp;

import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * if all query column are partition column with min/max aggregate function,
 * it can be rewritten from file_scan to tvf_relation
 */
public class PartitionAggregateRewrite extends OneRewriteRuleFactory {

    /**
     * Check if partition aggregate rewrite can be applied to the given aggregate node.
     *
     * @param agg the logical aggregate node to check
     * @return true if the rewrite can be applied, false otherwise
     */
    static boolean canApplyPartitionAggregateRewrite(LogicalAggregate<LogicalProject<LogicalFileScan>> agg) {
        Set<Class<? extends AggregateFunction>> functionClasses = agg.getAggregateFunctions()
                .stream()
                .map(AggregateFunction::getClass)
                .collect(Collectors.toSet());
        if (!PartitionAggOp.supportedFunctions().keySet().containsAll(functionClasses)) {
            return false;
        }

        LogicalProject<LogicalFileScan> project = agg.child();
        LogicalFileScan fileScan = project.child();

        if (!(fileScan.getTable() instanceof HMSExternalTable)) {
            return false;
        }

        Map<String, Slot> scanOutput = fileScan.getOutput()
                .stream()
                .collect(Collectors.toMap(slot -> slot.getName().toLowerCase(),
                        java.util.function.Function.identity()));
        Set<Slot> partitionSlots = ((HMSExternalTable) fileScan.getTable()).getPartitionColumns()
                .stream()
                .map(column -> scanOutput.get(column.getName().toLowerCase()))
                .collect(Collectors.toSet());

        return agg.isNormalized() && partitionSlots.containsAll(project.getOutput());
    }

    @Override
    public Rule build() {
        return logicalAggregate(
                    logicalProject(
                            logicalFileScan()
                    )
                ).when(PartitionAggregateRewrite::canApplyPartitionAggregateRewrite)
                .thenApply(ctx -> {
                    LogicalAggregate<LogicalProject<LogicalFileScan>> agg = ctx.root;
                    LogicalProject<LogicalFileScan> project = agg.child();
                    LogicalFileScan fileScan = project.child();
                    HMSExternalTable table = (HMSExternalTable) fileScan.getTable();
                    Optional<TableValuedFunction> tvf = ((HMSExternalCatalog) table.getDatabase().getCatalog())
                            .getMetaTableFunction(table.getDbName(),
                                    table.getName() + "$partitions");
                    if (tvf.isPresent()) {
                        return agg.withChildren(ImmutableList.of(
                                project.withChildren(
                                        ImmutableList.of(new LogicalTVFRelation(fileScan.getRelationId(), tvf.get(),
                                                project.getGroupExpression(),
                                                Optional.of(project.getLogicalProperties()))))
                        ));
                    }
                    return agg;
                }).toRule(RuleType.PARTITION_AGGREGATE_WITH_PROJECT_FOR_FILE_SCAN);
    }
}
