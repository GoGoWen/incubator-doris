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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.tablefunction.PartitionValuesTableValuedFunction.PartitionAggOp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Partition Aggregate Rewrite Rule
 *
 * When all queried columns are partition columns with supported aggregate functions (min/max),
 * the query can be optimized by rewriting from file scan to union with constant expressions.
 *
 * Transformation Pattern:
 * BEFORE:
 *   LogicalAggregate (min/max functions)
 *     └── LogicalProject (partition columns only)
 *           └── LogicalFileScan (HMS external table)
 *
 * AFTER:
 *   LogicalAggregate (min/max functions)
 *     └── LogicalProject (partition columns only)
 *           └── LogicalUnion (with constantExprsList containing partition values)
 *                 └── (no children - partition values stored in constantExprsList)
 *
 * This rule integrates the logic from MergeOneRowRelationIntoUnion to directly create
 * an optimized LogicalUnion with constantExprsList instead of creating LogicalOneRowRelation
 * children that would later need transformation.
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
                    List<Column> partitionColumns = ((HMSExternalTable) fileScan.getTable()).getPartitionColumns();
                    Map<String, DataType> nameToType = Maps.newHashMap();
                    for (NamedExpression expression : project.getProjects()) {
                        nameToType.put(expression.getName(), expression.getDataType());
                    }
                    Map<Long, PartitionItem> selectedPartitions;
                    if (fileScan.getSelectedPartitions().isPruned) {
                        selectedPartitions = fileScan.getSelectedPartitions().selectedPartitions;
                    } else {
                        HMSExternalTable hiveTbl = (HMSExternalTable) fileScan.getTable();
                        boolean isViewBased = hiveTbl.isViewBased();

                        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                                .getMetaStoreCache((HMSExternalCatalog) hiveTbl.getCatalog());

                        int partitionNum = isViewBased
                                ? cache.getPartitionNumFromView(hiveTbl.getDbName(), hiveTbl.getName())
                                : cache.getPartitionNum(hiveTbl.getDbName(), hiveTbl.getName());

                        HiveMetaStoreCache.HivePartitionValues hivePartitionValues;
                        if (partitionNum > Config.max_partition_num_for_single_hive_table_without_filter) {
                            hivePartitionValues = isViewBased
                                    ? cache.getPartitionValuesFromViewWithoutCache(hiveTbl.getDbName(),
                                            hiveTbl.getName(), hiveTbl.getPartitionColumnTypes())
                                    : cache.getPartitionValuesWithoutCache(hiveTbl.getDbName(),
                                            hiveTbl.getName(), hiveTbl.getPartitionColumnTypes());
                        } else {
                            hivePartitionValues = isViewBased
                                    ? cache.getPartitionValuesFromView(hiveTbl.getDbName(),
                                            hiveTbl.getName(), hiveTbl.getPartitionColumnTypes())
                                    : cache.getPartitionValues(hiveTbl.getDbName(),
                                            hiveTbl.getName(), hiveTbl.getPartitionColumnTypes());
                        }

                        selectedPartitions = hivePartitionValues.getIdToPartitionItem();
                    }

                    // Handle empty partition selection case - return empty relation
                    if (selectedPartitions.isEmpty()) {
                        return agg.withChildren(ImmutableList.of(
                                project.withChildren(ImmutableList.of(
                                    new LogicalEmptyRelation(ctx.statementContext.getNextRelationId(),
                                        project.getOutput())))));
                    }

                    List<NamedExpression> unionOutputs = new ArrayList<>(project.getProjects());

                    List<List<NamedExpression>> constantExprsList = buildConstantExpressionsFromPartitions(
                            selectedPartitions.values(), partitionColumns, nameToType, unionOutputs);

                    // Create LogicalUnion with constantExprsList instead of children
                    LogicalUnion union = new LogicalUnion(Qualifier.ALL, unionOutputs,
                            ImmutableList.of(), constantExprsList, false, ImmutableList.of());

                    return agg.withChildren(ImmutableList.of(
                            project.withChildren(ImmutableList.of(union))
                    ));
                }).toRule(RuleType.PARTITION_AGGREGATE_WITH_PROJECT_FOR_FILE_SCAN);
    }

    /**
     * Build constant expressions list from partition values with type coercion.
     * This integrates the logic from MergeOneRowRelationIntoUnion to directly create
     * a union with constantExprsList instead of creating LogicalOneRowRelation children.
     */
    private static List<List<NamedExpression>> buildConstantExpressionsFromPartitions(
            Collection<PartitionItem> selectedPartitions,
            List<Column> partitionColumns,
            Map<String, DataType> nameToType,
            List<NamedExpression> unionOutputs) {
        ImmutableList.Builder<List<NamedExpression>> constantExprsList = ImmutableList.builder();

        for (PartitionItem partitionItem : selectedPartitions) {
            PartitionKey partitionKey = ((ListPartitionItem) partitionItem).getItems().get(0);
            ImmutableList.Builder<NamedExpression> constantExprs = ImmutableList.builder();

            for (int i = 0; i < partitionColumns.size(); i++) {
                String name = partitionColumns.get(i).getName();
                if (nameToType.containsKey(name)) {
                    Literal literal = Literal.of(partitionKey.getKeys().get(i).getRealValue());
                    Expression castedLiteral = literal.checkedCastTo(nameToType.get(name));

                    // Find the corresponding output to get target type for proper coercion
                    DataType targetType = null;
                    for (NamedExpression output : unionOutputs) {
                        if (output.getName().equals(name)) {
                            targetType = output.getDataType();
                            break;
                        }
                    }

                    // Apply type coercion like @{MergeOneRowRelationIntoUnion} does
                    if (targetType != null && !castedLiteral.getDataType().equals(targetType)) {
                        castedLiteral = TypeCoercionUtils.castIfNotSameType(castedLiteral, targetType);
                    }

                    constantExprs.add(new Alias(castedLiteral, name));
                }
            }
            constantExprsList.add(constantExprs.build());
        }

        return constantExprsList.build();
    }
}
