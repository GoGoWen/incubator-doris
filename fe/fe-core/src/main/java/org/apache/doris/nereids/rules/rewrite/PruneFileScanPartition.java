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

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruneExpressionExtractor;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner;
import org.apache.doris.nereids.rules.expression.rules.PredicateRewriteForPartitionFilter;
import org.apache.doris.nereids.rules.expression.rules.PredicateRewriteForPartitionPrune;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Used to prune partition of file scan. For different external tables, there is no unified partition prune method.
 * For example, Hive is using hive meta store api to get partitions. Iceberg is using Iceberg api to get FileScanTask,
 * which doesn't return a partition list.
 * So here we only support Hive table partition prune.
 * For other external table, simply pass the conjuncts to LogicalFileScan, so that different
 * external file ScanNode could do the partition filter by themselves.
 */
public class PruneFileScanPartition extends OneRewriteRuleFactory {

    private static final Logger LOG = LogManager.getLogger(PruneFileScanPartition.class);

    private static final String QUERY_FILTER_PARTITION_SQL = "SELECT * FROM ${catalogName}.${dbName}"
            + ".${tblName}$partitions where ${filterSql}";

    @Override
    public Rule build() {
        return logicalFilter(logicalFileScan()).whenNot(p -> p.child().getSelectedPartitions().isPruned)
                .thenApply(ctx -> {
                    LogicalFilter<LogicalFileScan> filter = ctx.root;
                    LogicalFileScan scan = filter.child();
                    ExternalTable tbl = scan.getTable();

                    SelectedPartitions selectedPartitions;
                    // TODO(cmy): support other external table
                    if (tbl instanceof HMSExternalTable && ((HMSExternalTable) tbl).getDlaType() == DLAType.HIVE) {
                        HMSExternalTable hiveTbl = (HMSExternalTable) tbl;
                        selectedPartitions = pruneHivePartitions(hiveTbl, filter, scan,
                            ctx.cascadesContext, hiveTbl.isViewBased());
                    } else {
                        // set isPruned so that it won't go pass the partition prune again
                        selectedPartitions = new SelectedPartitions(0, ImmutableMap.of(), true);
                    }

                    LogicalFileScan rewrittenScan = scan.withConjuncts(filter.getConjuncts())
                            .withSelectedPartitions(selectedPartitions);
                    return new LogicalFilter<>(filter.getConjuncts(), rewrittenScan);
                }).toRule(RuleType.FILE_SCAN_PARTITION_PRUNE);
    }

    private SelectedPartitions pruneHivePartitionsFromView(HMSExternalTable hiveTbl,
                                                   LogicalFilter<LogicalFileScan> filter, LogicalFileScan scan,
                                                   CascadesContext ctx) {
        Map<Long, PartitionItem> selectedPartitionItems = Maps.newHashMap();
        Map<String, Slot> scanOutput = scan.getOutput()
                .stream()
                .collect(Collectors.toMap(slot -> slot.getName().toLowerCase(), Function.identity()));

        List<Slot> partitionSlots = hiveTbl.getPartitionColumns()
                .stream()
                .map(column -> scanOutput.get(column.getName().toLowerCase()))
                .collect(Collectors.toList());

        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) hiveTbl.getCatalog());
        int partitionNum = cache.getPartitionNumFromView(hiveTbl.getDbName(), hiveTbl.getName());
        boolean isDirectlyByFilter = partitionNum > Config.max_partition_num_for_single_hive_table_without_filter;
        Expression partitionPredicate = null;
        // use listPartitionsByFilterFromView
        if (isDirectlyByFilter) {
            try {
                partitionPredicate = PartitionPruneExpressionExtractor.extract(filter.getPredicate(),
                        ImmutableSet.copyOf(partitionSlots), ctx);
                List<String> partitionColumnNames = partitionSlots.stream().map(e -> e.getName())
                        .collect(Collectors.toList());
                partitionPredicate = PredicateRewriteForPartitionPrune.rewrite(partitionPredicate, ctx);
                partitionPredicate = PredicateRewriteForPartitionFilter.rewrite(partitionPredicate, ctx);
                if (BooleanLiteral.TRUE.equals(partitionPredicate)) {
                    HiveMetaStoreCache.HivePartitionValues hivePartitionValues = cache.getPartitionValuesFromView(
                            hiveTbl.getDbName(), hiveTbl.getName(), hiveTbl.getPartitionColumnTypes());
                    selectedPartitionItems = hivePartitionValues.getIdToPartitionItem();
                } else if (BooleanLiteral.FALSE.equals(partitionPredicate) || partitionPredicate.isNullLiteral()) {
                    // do nothing
                } else {
                    selectedPartitionItems = cache.getPartitionValuesByFilterFromView(hiveTbl.getDbName(),
                            hiveTbl.getName(),
                            partitionPredicate.toSql(), partitionColumnNames, hiveTbl.getPartitionColumnTypes());
                }
            } catch (Exception e) {
                // for some complex case listPartitionsByFilterView may not support
                LOG.warn("get selected partition items by listPartitionsByFilterFromView failed, filter sql is "
                        + partitionPredicate.toSql() + ", use getPartitionValuesFromView instead");
                isDirectlyByFilter = false;
            }
        }
        if (!isDirectlyByFilter) {
            ///
            HiveMetaStoreCache.HivePartitionValues hivePartitionValues = cache.getPartitionValuesFromView(
                    hiveTbl.getDbName(), hiveTbl.getName(), hiveTbl.getPartitionColumnTypes());
            Map<Long, PartitionItem> idToPartitionItem = hivePartitionValues.getIdToPartitionItem();
            List<Long> prunedPartitions = new ArrayList<>(PartitionPruner.prune(
                    partitionSlots, filter.getPredicate(), idToPartitionItem, ctx));
            for (Long id : prunedPartitions) {
                selectedPartitionItems.put(id, idToPartitionItem.get(id));
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("[ViewBased] db:{} table: {} total partition num: {} selected partitions: {}",
                        hiveTbl.getDbName(), hiveTbl.getName(), partitionNum, selectedPartitionItems.size());
            }
        }
        return new SelectedPartitions(partitionNum, selectedPartitionItems, true);
    }

    private boolean isFilterSupportedByListPartitions(Expression expression) {
        if (expression instanceof SlotReference) {
            return true;
        }
        if (expression instanceof Literal) {
            return true;
        }
        if (expression instanceof And || expression instanceof Or || expression instanceof EqualTo
                || expression instanceof GreaterThan || expression instanceof GreaterThanEqual
                || expression instanceof LessThan || expression instanceof LessThanEqual) {
            BinaryOperator binaryOperator = (BinaryOperator) expression;
            return isFilterSupportedByListPartitions(binaryOperator.left())
                    && isFilterSupportedByListPartitions(binaryOperator.right());
        }
        return false;
    }

    private SelectedPartitions pruneHivePartitions(HMSExternalTable hiveTbl,
            LogicalFilter<LogicalFileScan> filter, LogicalFileScan scan, CascadesContext ctx, boolean isViewBased) {
        Map<Long, PartitionItem> selectedPartitionItems = Maps.newHashMap();
        if (CollectionUtils.isEmpty(hiveTbl.getPartitionColumns())) {
            // non partitioned table, return NOT_PRUNED.
            // non partition table will be handled in HiveScanNode.
            return SelectedPartitions.NOT_PRUNED;
        }
        if (isViewBased) {
            return pruneHivePartitionsFromView(hiveTbl, filter, scan, ctx);
        }
        Map<String, Slot> scanOutput = scan.getOutput()
                .stream()
                .collect(Collectors.toMap(slot -> slot.getName().toLowerCase(), Function.identity()));

        List<Slot> partitionSlots = hiveTbl.getPartitionColumns()
                .stream()
                .map(column -> scanOutput.get(column.getName().toLowerCase()))
                .collect(Collectors.toList());

        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) hiveTbl.getCatalog());
        int partitionNum = cache.getPartitionNum(hiveTbl.getDbName(), hiveTbl.getName());
        boolean isDirectlyByFilter = true;
        Expression partitionPredicate = null;
        // use listPartitionsByFilter
        try {
            partitionPredicate = PartitionPruneExpressionExtractor.extract(filter.getPredicate(),
                    ImmutableSet.copyOf(partitionSlots), ctx);
            List<String> partitionColumnNames = partitionSlots.stream().map(e -> e.getName())
                    .collect(Collectors.toList());
            partitionPredicate = PredicateRewriteForPartitionPrune.rewrite(partitionPredicate, ctx);
            partitionPredicate = PredicateRewriteForPartitionFilter.rewrite(partitionPredicate, ctx);

            if (BooleanLiteral.TRUE.equals(partitionPredicate)) {
                HiveMetaStoreCache.HivePartitionValues hivePartitionValues = cache.getPartitionValues(
                        hiveTbl.getDbName(), hiveTbl.getName(), hiveTbl.getPartitionColumnTypes());
                selectedPartitionItems = hivePartitionValues.getIdToPartitionItem();
            } else if (BooleanLiteral.FALSE.equals(partitionPredicate) || partitionPredicate.isNullLiteral()) {
                // do nothing
            } else {
                if (partitionNum > Config.max_partition_num_for_single_hive_table_without_filter
                        && isFilterSupportedByListPartitions(partitionPredicate)) {
                    selectedPartitionItems = cache.getPartitionValuesByFilter(hiveTbl.getDbName(),
                            hiveTbl.getName(), partitionPredicate.toSql(), partitionColumnNames,
                                hiveTbl.getPartitionColumnTypes());
                } else {
                    HiveMetaStoreCache.HivePartitionValues hivePartitionValues = cache.getPartitionValues(
                            hiveTbl.getDbName(), hiveTbl.getName(), hiveTbl.getPartitionColumnTypes());
                    Map<Long, PartitionItem> idToPartitionItem = hivePartitionValues.getIdToPartitionItem();
                    partitionNum = idToPartitionItem.size();
                    List<Long> prunedPartitions = Lists.newArrayList();
                    if (PartitionPruner.tryPrune(partitionSlots, filter.getPredicate(), idToPartitionItem,
                            prunedPartitions, ctx)) {
                        for (Long id : prunedPartitions) {
                            selectedPartitionItems.put(id, idToPartitionItem.get(id));
                        }
                    } else {
                        isDirectlyByFilter = false;
                    }
                }
            }
        } catch (Exception e) {
            // for some complex case listPartitionsByFilter may not support
            LOG.warn("get selected partition items by listPartitionsByFilter failed, filter sql is "
                    + partitionPredicate.toSql() + ", use getPartitionValues instead");
            isDirectlyByFilter = false;
        }
        if (!isDirectlyByFilter) {
            try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext()) {
                partitionPredicate = PartitionPruneExpressionExtractor.extract(filter.getPredicate(),
                        ImmutableSet.copyOf(partitionSlots), ctx);
                r.connectContext.setBdpAuthContext(ConnectContext.get().getBdpAuthContext());
                Map<String, String> params = new HashMap<>();
                params.put("catalogName", hiveTbl.getCatalog().getName());
                params.put("dbName", hiveTbl.getDbName());
                params.put("tblName", hiveTbl.getName());
                params.put("filterSql", partitionPredicate.toSql());
                StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
                String sql = stringSubstitutor.replace(QUERY_FILTER_PARTITION_SQL);
                List<ResultRow> partitionRows = new StmtExecutor(r.connectContext, sql).executeInternalQuery();
                selectedPartitionItems = Maps.newHashMapWithExpectedSize(partitionRows.size());
                for (ResultRow partition : partitionRows) {
                    List<PartitionValue> values = Lists.newArrayListWithExpectedSize(partitionSlots.size());
                    for (String partitionValue : partition.getValues()) {
                        values.add(new PartitionValue(partitionValue,
                                HiveMetaStoreCache.HIVE_DEFAULT_PARTITION.equals(partitionValue)));
                    }
                    try {
                        PartitionKey partitionKey = PartitionKey.createListPartitionKeyWithTypes(values,
                                hiveTbl.getPartitionColumnTypes(), true);
                        String partitionName = IntStream.range(0, partitionSlots.size())
                                .mapToObj(i -> partitionSlots.get(i).getName() + "=" + values.get(i).getStringValue())
                                .collect(Collectors.joining("/"));
                        long partitionId = Util.genIdByName(hiveTbl.getCatalog().getName(), hiveTbl.getDbName(),
                                hiveTbl.getName(), partitionName);
                        selectedPartitionItems.put(partitionId,
                                new ListPartitionItem(Lists.newArrayList(partitionKey)));
                    } catch (AnalysisException e) {
                        throw new CacheException("failed to convert hive partition %s to list partition in catalog %s",
                                e, partition.getValues(), hiveTbl.getCatalog().getName());
                    }
                }
            } catch (Exception e) {
                LOG.warn("failed to fetch filter partitions for " + hiveTbl.getDbName() + "." + hiveTbl.getName());
                throw e;
            }
        }
        return new SelectedPartitions(partitionNum, selectedPartitionItems, true);
    }
}
