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

package org.apache.doris.catalog.external;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.HudiUtils;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSCachedClient;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.external.iceberg.util.IcebergUtils;
import org.apache.doris.mtmv.MTMVMaxTimestampSnapshot;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVTimestampSnapshot;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.HMSAnalysisTask;
import org.apache.doris.statistics.StatsType;
import org.apache.doris.statistics.TableStatsMeta;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Hive metastore external table.
 */
public class HMSExternalTable extends ExternalTable implements MTMVRelatedTableIf {
    private static final Logger LOG = LogManager.getLogger(HMSExternalTable.class);

    public static final Set<String> SUPPORTED_HIVE_FILE_FORMATS;
    public static final Set<String> SUPPORTED_HIVE_TRANSACTIONAL_FILE_FORMATS;
    public static final Set<String> SUPPORTED_HUDI_FILE_FORMATS;

    private static final Map<StatsType, String> MAP_SPARK_STATS_TO_DORIS;

    private static final String TBL_PROP_TRANSIENT_LAST_DDL_TIME = "transient_lastDdlTime";

    private static final String NUM_ROWS = "numRows";

    private static final String SPARK_COL_STATS = "spark.sql.statistics.colStats.";
    private static final String SPARK_STATS_MAX = ".max";
    private static final String SPARK_STATS_MIN = ".min";
    private static final String SPARK_STATS_NDV = ".distinctCount";
    private static final String SPARK_STATS_NULLS = ".nullCount";
    private static final String SPARK_STATS_AVG_LEN = ".avgLen";
    private static final String SPARK_STATS_MAX_LEN = ".avgLen";
    private static final String SPARK_STATS_HISTOGRAM = ".histogram";

    static {
        SUPPORTED_HIVE_FILE_FORMATS = Sets.newHashSet();
        SUPPORTED_HIVE_FILE_FORMATS.add("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        SUPPORTED_HIVE_FILE_FORMATS.add("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
        SUPPORTED_HIVE_FILE_FORMATS.add("org.apache.hadoop.mapred.TextInputFormat");
        // Some hudi table use HoodieParquetInputFormatBase as input format
        // But we can't treat it as hudi table.
        // So add to SUPPORTED_HIVE_FILE_FORMATS and treat is as a hive table.
        // Then Doris will just list the files from location and read parquet files directly.
        SUPPORTED_HIVE_FILE_FORMATS.add("org.apache.hudi.hadoop.HoodieParquetInputFormatBase");

        SUPPORTED_HIVE_TRANSACTIONAL_FILE_FORMATS = Sets.newHashSet();
        SUPPORTED_HIVE_TRANSACTIONAL_FILE_FORMATS.add("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
    }

    static {
        SUPPORTED_HUDI_FILE_FORMATS = Sets.newHashSet();
        SUPPORTED_HUDI_FILE_FORMATS.add("org.apache.hudi.hadoop.HoodieParquetInputFormat");
        SUPPORTED_HUDI_FILE_FORMATS.add("com.uber.hoodie.hadoop.HoodieInputFormat");
        SUPPORTED_HUDI_FILE_FORMATS.add("org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        SUPPORTED_HUDI_FILE_FORMATS.add("com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat");
    }

    static {
        MAP_SPARK_STATS_TO_DORIS = Maps.newHashMap();
        MAP_SPARK_STATS_TO_DORIS.put(StatsType.NDV, SPARK_STATS_NDV);
        MAP_SPARK_STATS_TO_DORIS.put(StatsType.AVG_SIZE, SPARK_STATS_AVG_LEN);
        MAP_SPARK_STATS_TO_DORIS.put(StatsType.MAX_SIZE, SPARK_STATS_MAX_LEN);
        MAP_SPARK_STATS_TO_DORIS.put(StatsType.NUM_NULLS, SPARK_STATS_NULLS);
        MAP_SPARK_STATS_TO_DORIS.put(StatsType.MIN_VALUE, SPARK_STATS_MIN);
        MAP_SPARK_STATS_TO_DORIS.put(StatsType.MAX_VALUE, SPARK_STATS_MAX);
        MAP_SPARK_STATS_TO_DORIS.put(StatsType.HISTOGRAM, SPARK_STATS_HISTOGRAM);
    }

    private volatile org.apache.hadoop.hive.metastore.api.Table remoteTable = null;
    private List<Column> partitionColumns;

    private DLAType dlaType = DLAType.UNKNOWN;

    // No as precise as row count in TableStats, but better than none.
    private long estimatedRowCount = -1;

    // record the event update time when enable hms event listener
    protected volatile long eventUpdateTime;

    public enum DLAType {
        UNKNOWN, HIVE, HUDI, ICEBERG
    }

    /**
     * Create hive metastore external table.
     *
     * @param id Table id.
     * @param name Table name.
     * @param dbName Database name.
     * @param catalog HMSExternalCatalog.
     */
    public HMSExternalTable(long id, String name, String dbName, HMSExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.HMS_EXTERNAL_TABLE);
    }

    public boolean isSupportedHmsTable() {
        try {
            makeSureInitialized();
            return true;
        } catch (NotSupportedException e) {
            LOG.warn("Not supported hms table, message: {}", e.getMessage());
            return false;
        }
    }

    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            remoteTable = ((HMSExternalCatalog) catalog).getClient().getTable(dbName, name);
            if (remoteTable == null) {
                throw new IllegalArgumentException("Hms table not exists, table: " + getNameWithFullQualifiers());
            } else {
                if (supportedIcebergTable()) {
                    dlaType = DLAType.ICEBERG;
                } else if (supportedHoodieTable()) {
                    dlaType = DLAType.HUDI;
                } else if (supportedHiveTable()) {
                    dlaType = DLAType.HIVE;
                } else {
                    throw new NotSupportedException("Unsupported dlaType for table: " + getNameWithFullQualifiers());
                }
            }
            objectCreated = true;
            estimatedRowCount = getRowCountFromExternalSource(true);
        }
    }

    /**
     * Now we only support cow table in iceberg.
     */
    private boolean supportedIcebergTable() {
        Map<String, String> paras = remoteTable.getParameters();
        if (paras == null) {
            return false;
        }
        return paras.containsKey("table_type") && paras.get("table_type").equalsIgnoreCase("ICEBERG");
    }

    /**
     * `HoodieParquetInputFormat`: `Snapshot Queries` on cow and mor table and `Read Optimized Queries` on cow table
     */
    private boolean supportedHoodieTable() {
        if (remoteTable.getSd() == null) {
            return false;
        }
        String inputFormatName = remoteTable.getSd().getInputFormat();
        return inputFormatName != null && SUPPORTED_HUDI_FILE_FORMATS.contains(inputFormatName);
    }

    public boolean isHoodieCowTable() {
        if (remoteTable.getSd() == null) {
            return false;
        }
        String inputFormatName = remoteTable.getSd().getInputFormat();
        return "org.apache.hudi.hadoop.HoodieParquetInputFormat".equals(inputFormatName);
    }

    /**
     * Now we only support three file input format hive tables: parquet/orc/text.
     * Support managed_table and external_table.
     */
    private boolean supportedHiveTable() {
        // we will return false if null, which means that the table type maybe unsupported.
        if (remoteTable.getSd() == null) {
            return false;
        }
        String inputFileFormat = remoteTable.getSd().getInputFormat();
        if (inputFileFormat == null) {
            return false;
        }
        boolean supportedFileFormat = SUPPORTED_HIVE_FILE_FORMATS.contains(inputFileFormat);
        if (!supportedFileFormat) {
            // for easier debugging, need return error message if unsupported input format is used.
            // NotSupportedException is required by some operation.
            throw new NotSupportedException("Unsupported hive input format: " + inputFileFormat);
        }
        LOG.debug("hms table {} is {} with file format: {}", name, remoteTable.getTableType(), inputFileFormat);
        return true;
    }

    /**
     * Get the related remote hive metastore table.
     */
    public org.apache.hadoop.hive.metastore.api.Table getRemoteTable() {
        makeSureInitialized();
        return remoteTable;
    }

    public List<Type> getPartitionColumnTypes() {
        makeSureInitialized();
        getFullSchema();
        return partitionColumns.stream().map(c -> c.getType()).collect(Collectors.toList());
    }

    @Override
    public List<Column> getPartitionColumns() {
        makeSureInitialized();
        getFullSchema();
        return partitionColumns;
    }

    public boolean isHiveTransactionalTable() {
        return dlaType == DLAType.HIVE && AcidUtils.isTransactionalTable(remoteTable)
                && isSupportedTransactionalFileFormat();
    }

    private boolean isSupportedTransactionalFileFormat() {
        // Sometimes we meet "transactional" = "true" but format is parquet, which is not supported.
        // So we need to check the input format for transactional table.
        String inputFormatName = remoteTable.getSd().getInputFormat();
        return inputFormatName != null && SUPPORTED_HIVE_TRANSACTIONAL_FILE_FORMATS.contains(inputFormatName);
    }

    public boolean isFullAcidTable() {
        return dlaType == DLAType.HIVE && AcidUtils.isFullAcidTable(remoteTable);
    }

    @Override
    public boolean isView() {
        makeSureInitialized();
        return remoteTable.isSetViewOriginalText() || remoteTable.isSetViewExpandedText();
    }

    @Override
    public String getEngine() {
        switch (type) {
            case HIVE:
                return "Hive";
            case ICEBERG:
                return "Iceberg";
            case HUDI:
                return "Hudi";
            default:
                return null;
        }
    }

    @Override
    public String getComment() {
        return "";
    }

    @Override
    public long getCreateTime() {
        return 0;
    }

    @Override
    public long getRowCount() {
        makeSureInitialized();
        long rowCount = getRowCountFromExternalSource(false);
        if (rowCount == -1) {
            LOG.debug("Will estimate row count from file list.");
            rowCount = StatisticsUtil.getRowCountFromFileList(this);
        }
        return rowCount;
    }

    private long getRowCountFromExternalSource(boolean isInit) {
        long rowCount;
        switch (dlaType) {
            case HIVE:
                rowCount = StatisticsUtil.getHiveRowCount(this, isInit);
                break;
            case ICEBERG:
                rowCount = StatisticsUtil.getIcebergRowCount(this);
                break;
            default:
                LOG.debug("getRowCount for dlaType {} is not supported.", dlaType);
                rowCount = -1;
        }
        return rowCount;
    }

    @Override
    public long getDataLength() {
        return 0;
    }

    @Override
    public long getAvgRowLength() {
        return 0;
    }

    public long getLastCheckTime() {
        return 0;
    }

    /**
     * get the dla type for scan node to get right information.
     */
    public DLAType getDlaType() {
        makeSureInitialized();
        return dlaType;
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                getName(), dbName);
        tTableDescriptor.setHiveTable(tHiveTable);
        return tTableDescriptor;
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new HMSAnalysisTask(info);
    }

    public String getViewText() {
        String viewText = getViewExpandedText();
        if (StringUtils.isNotEmpty(viewText)) {
            return viewText;
        }
        return getViewOriginalText();
    }

    public String getViewExpandedText() {
        LOG.debug("View expanded text of hms table [{}.{}.{}] : {}",
                this.getCatalog().getName(), this.getDbName(), this.getName(), remoteTable.getViewExpandedText());
        return remoteTable.getViewExpandedText();
    }

    public String getViewOriginalText() {
        LOG.debug("View original text of hms table [{}.{}.{}] : {}",
                this.getCatalog().getName(), this.getDbName(), this.getName(), remoteTable.getViewOriginalText());
        return remoteTable.getViewOriginalText();
    }

    public String getMetastoreUri() {
        return ((HMSExternalCatalog) catalog).getHiveMetastoreUris();
    }

    public Map<String, String> getCatalogProperties() {
        return catalog.getProperties();
    }

    public Map<String, String> getHadoopProperties() {
        return catalog.getCatalogProperty().getHadoopProperties();
    }

    public List<ColumnStatisticsObj> getHiveTableColumnStats(List<String> columns) {
        HMSCachedClient client = ((HMSExternalCatalog) catalog).getClient();
        return client.getTableColumnStatistics(dbName, name, columns);
    }

    public Map<String, List<ColumnStatisticsObj>> getHivePartitionColumnStats(
            List<String> partNames, List<String> columns) {
        HMSCachedClient client = ((HMSExternalCatalog) catalog).getClient();
        return client.getPartitionColumnStatistics(dbName, name, partNames, columns);
    }

    public Partition getPartition(List<String> partitionValues) {
        HMSCachedClient client = ((HMSExternalCatalog) catalog).getClient();
        return client.getPartition(dbName, name, partitionValues);
    }

    @Override
    public Set<String> getPartitionNames() {
        makeSureInitialized();
        HMSCachedClient client = ((HMSExternalCatalog) catalog).getClient();
        List<String> names = client.listPartitionNames(dbName, name);
        return new HashSet<>(names);
    }

    @Override
    public List<Column> initSchemaAndUpdateTime() {
        org.apache.hadoop.hive.metastore.api.Table table = ((HMSExternalCatalog) catalog).getClient()
                .getTable(dbName, name);
        // try to use transient_lastDdlTime from hms client
        schemaUpdateTime = MapUtils.isNotEmpty(table.getParameters())
                && table.getParameters().containsKey(TBL_PROP_TRANSIENT_LAST_DDL_TIME)
                ? Long.parseLong(table.getParameters().get(TBL_PROP_TRANSIENT_LAST_DDL_TIME)) * 1000
                // use current timestamp if lastDdlTime does not exist (hive views don't have this prop)
                : System.currentTimeMillis();
        return initSchema();
    }

    public long getLastDdlTime() {
        makeSureInitialized();
        Map<String, String> parameters = remoteTable.getParameters();
        if (parameters == null || !parameters.containsKey(TBL_PROP_TRANSIENT_LAST_DDL_TIME)) {
            return 0L;
        }
        return Long.parseLong(parameters.get(TBL_PROP_TRANSIENT_LAST_DDL_TIME)) * 1000;
    }

    @Override
    public List<Column> initSchema() {
        makeSureInitialized();
        List<Column> columns;
        if (dlaType.equals(DLAType.ICEBERG)) {
            columns = getIcebergSchema();
        } else if (dlaType.equals(DLAType.HUDI)) {
            columns = getHudiSchema();
        } else {
            columns = getHiveSchema();
        }
        initPartitionColumns(columns);
        return columns;
    }

    private List<Column> getIcebergSchema() {
        return IcebergUtils.getSchema(catalog, dbName, name);
    }

    private List<Column> getHudiSchema() {
        org.apache.avro.Schema hudiSchema = HiveMetaStoreClientHelper.getHudiTableSchema(this);
        List<Column> tmpSchema = Lists.newArrayListWithCapacity(hudiSchema.getFields().size());
        for (org.apache.avro.Schema.Field hudiField : hudiSchema.getFields()) {
            String columnName = hudiField.name().toLowerCase(Locale.ROOT);
            tmpSchema.add(new Column(columnName, HudiUtils.fromAvroHudiTypeToDorisType(hudiField.schema()),
                    true, null, true, null, "", true, null, -1, null));
        }
        return tmpSchema;
    }

    private List<Column> getHiveSchema() {
        List<Column> columns;
        List<FieldSchema> schema = ((HMSExternalCatalog) catalog).getClient().getSchema(dbName, name);
        List<Column> tmpSchema = Lists.newArrayListWithCapacity(schema.size());
        for (FieldSchema field : schema) {
            tmpSchema.add(new Column(field.getName().toLowerCase(Locale.ROOT),
                    HiveMetaStoreClientHelper.hiveTypeToDorisType(field.getType()), true, null,
                    true, field.getComment(), true, -1));
        }
        columns = tmpSchema;
        return columns;
    }

    @Override
    public long getCacheRowCount() {
        //Cached accurate information
        TableStatsMeta tableStats = Env.getCurrentEnv().getAnalysisManager().findTableStatsStatus(id);
        if (tableStats != null) {
            long rowCount = tableStats.rowCount;
            LOG.debug("Estimated row count for db {} table {} is {}.", dbName, name, rowCount);
            return rowCount;
        }

        //estimated information
        if (estimatedRowCount != -1) {
            return estimatedRowCount;
        }
        return -1;
    }

    @Override
    public long estimatedRowCount() {
        try {
            TableStatsMeta tableStats = Env.getCurrentEnv().getAnalysisManager().findTableStatsStatus(id);
            if (tableStats != null) {
                long rowCount = tableStats.rowCount;
                LOG.debug("Estimated row count for db {} table {} is {}.", dbName, name, rowCount);
                return rowCount;
            }

            if (estimatedRowCount != -1) {
                return estimatedRowCount;
            }
            // Cache the estimated row count in this structure
            // though the table never get analyzed, since the row estimation might be expensive caused by RPC.
            estimatedRowCount = getRowCount();
            return estimatedRowCount;
        } catch (Exception e) {
            LOG.warn("Fail to get row count for table {}", name, e);
        }
        return 1;
    }

    private void initPartitionColumns(List<Column> schema) {
        List<String> partitionKeys = remoteTable.getPartitionKeys().stream().map(FieldSchema::getName)
                .collect(Collectors.toList());
        partitionColumns = Lists.newArrayListWithCapacity(partitionKeys.size());
        for (String partitionKey : partitionKeys) {
            // Do not use "getColumn()", which will cause dead loop
            for (Column column : schema) {
                if (partitionKey.equalsIgnoreCase(column.getName())) {
                    // For partition column, if it is string type, change it to varchar(65535)
                    // to be same as doris managed table.
                    // This is to avoid some unexpected behavior such as different partition pruning result
                    // between doris managed table and external table.
                    if (column.getType().getPrimitiveType() == PrimitiveType.STRING) {
                        column.setType(ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH));
                    }
                    partitionColumns.add(column);
                    break;
                }
            }
        }
        LOG.debug("get {} partition columns for table: {}", partitionColumns.size(), name);
    }

    public boolean hasColumnStatistics(String colName) {
        Map<String, String> parameters = remoteTable.getParameters();
        return parameters.keySet().stream()
                .filter(k -> k.startsWith(SPARK_COL_STATS + colName + ".")).findAny().isPresent();
    }

    public boolean fillColumnStatistics(String colName, Map<StatsType, String> statsTypes, Map<String, String> stats) {
        makeSureInitialized();
        if (!hasColumnStatistics(colName)) {
            return false;
        }

        Map<String, String> parameters = remoteTable.getParameters();
        for (StatsType type : statsTypes.keySet()) {
            String key = SPARK_COL_STATS + colName + MAP_SPARK_STATS_TO_DORIS.getOrDefault(type, "-");
            if (parameters.containsKey(key)) {
                stats.put(statsTypes.get(type), parameters.get(key));
            } else {
                // should not happen, spark would have all type (except histogram)
                stats.put(statsTypes.get(type), "NULL");
            }
        }
        return true;
    }

    @Override
    public Optional<ColumnStatistic> getColumnStatistic(String colName) {
        makeSureInitialized();
        switch (dlaType) {
            case HIVE:
                return getHiveColumnStats(colName);
            case ICEBERG:
                return StatisticsUtil.getIcebergColumnStats(colName,
                        Env.getCurrentEnv().getExtMetaCacheMgr().getIcebergMetadataCache().getIcebergTable(
                                catalog, dbName, name
                        ));
            default:
                LOG.warn("get column stats for dlaType {} is not supported.", dlaType);
        }
        return Optional.empty();
    }

    private Optional<ColumnStatistic> getHiveColumnStats(String colName) {
        List<ColumnStatisticsObj> tableStats = getHiveTableColumnStats(Lists.newArrayList(colName));
        if (tableStats == null || tableStats.isEmpty()) {
            LOG.debug(String.format("No table stats found in Hive metastore for column %s in table %s.",
                    colName, name));
            return Optional.empty();
        }

        Column column = getColumn(colName);
        if (column == null) {
            LOG.warn(String.format("No column %s in table %s.", colName, name));
            return Optional.empty();
        }
        Map<String, String> parameters = remoteTable.getParameters();
        ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder();
        double count = parameters.containsKey(NUM_ROWS) ? Double.parseDouble(parameters.get(NUM_ROWS)) : 0;
        columnStatisticBuilder.setCount(count);
        // The tableStats length is at most 1.
        for (ColumnStatisticsObj tableStat : tableStats) {
            if (!tableStat.isSetStatsData()) {
                continue;
            }
            ColumnStatisticsData data = tableStat.getStatsData();
            try {
                setStatData(column, data, columnStatisticBuilder, count);
            } catch (AnalysisException e) {
                LOG.debug(e);
                return Optional.empty();
            }
        }

        return Optional.of(columnStatisticBuilder.build());
    }

    private void setStatData(Column col, ColumnStatisticsData data, ColumnStatisticBuilder builder, double count)
            throws AnalysisException {
        long ndv = 0;
        long nulls = 0;
        String min = "";
        String max = "";
        double colSize = 0;
        if (!data.isSetStringStats()) {
            colSize = count * col.getType().getSlotSize();
        }
        // Collect ndv, nulls, min and max for different data type.
        if (data.isSetLongStats()) {
            LongColumnStatsData longStats = data.getLongStats();
            ndv = longStats.getNumDVs();
            nulls = longStats.getNumNulls();
            min = String.valueOf(longStats.getLowValue());
            max = String.valueOf(longStats.getHighValue());
        } else if (data.isSetStringStats()) {
            StringColumnStatsData stringStats = data.getStringStats();
            ndv = stringStats.getNumDVs();
            nulls = stringStats.getNumNulls();
            double avgColLen = stringStats.getAvgColLen();
            colSize = Math.round(avgColLen * count);
        } else if (data.isSetDecimalStats()) {
            DecimalColumnStatsData decimalStats = data.getDecimalStats();
            ndv = decimalStats.getNumDVs();
            nulls = decimalStats.getNumNulls();
            if (decimalStats.isSetLowValue()) {
                Decimal lowValue = decimalStats.getLowValue();
                if (lowValue != null) {
                    BigDecimal lowDecimal = new BigDecimal(new BigInteger(lowValue.getUnscaled()), lowValue.getScale());
                    min = lowDecimal.toString();
                }
            }
            if (decimalStats.isSetHighValue()) {
                Decimal highValue = decimalStats.getHighValue();
                if (highValue != null) {
                    BigDecimal highDecimal =
                            new BigDecimal(new BigInteger(highValue.getUnscaled()), highValue.getScale());
                    max = highDecimal.toString();
                }
            }
        } else if (data.isSetDoubleStats()) {
            DoubleColumnStatsData doubleStats = data.getDoubleStats();
            ndv = doubleStats.getNumDVs();
            nulls = doubleStats.getNumNulls();
            min = String.valueOf(doubleStats.getLowValue());
            max = String.valueOf(doubleStats.getHighValue());
        } else if (data.isSetDateStats()) {
            DateColumnStatsData dateStats = data.getDateStats();
            ndv = dateStats.getNumDVs();
            nulls = dateStats.getNumNulls();
            if (dateStats.isSetLowValue()) {
                org.apache.hadoop.hive.metastore.api.Date lowValue = dateStats.getLowValue();
                if (lowValue != null) {
                    LocalDate lowDate = LocalDate.ofEpochDay(lowValue.getDaysSinceEpoch());
                    min = lowDate.toString();
                }
            }
            if (dateStats.isSetHighValue()) {
                org.apache.hadoop.hive.metastore.api.Date highValue = dateStats.getHighValue();
                if (highValue != null) {
                    LocalDate highDate = LocalDate.ofEpochDay(highValue.getDaysSinceEpoch());
                    max = highDate.toString();
                }
            }
        } else {
            LOG.debug(String.format("Not suitable data type for column %s", col.getName()));
            throw new RuntimeException("Not supported data type.");
        }
        builder.setNdv(ndv);
        builder.setNumNulls(nulls);
        builder.setDataSize(colSize);
        builder.setAvgSizeByte(colSize / count);
        if (!min.equals("")) {
            builder.setMinValue(StatisticsUtil.convertToDouble(col.getType(), min));
            builder.setMinExpr(StatisticsUtil.readableValue(col.getType(), min));
        } else {
            builder.setMinValue(Double.MIN_VALUE);
        }
        if (!max.equals("")) {
            builder.setMaxValue(StatisticsUtil.convertToDouble(col.getType(), max));
            builder.setMaxExpr(StatisticsUtil.readableValue(col.getType(), max));
        } else {
            builder.setMaxValue(Double.MAX_VALUE);
        }
    }

    public void setEventUpdateTime(long updateTime) {
        this.eventUpdateTime = updateTime;
    }

    @Override
    // get the max value of `schemaUpdateTime` and `eventUpdateTime`
    // eventUpdateTime will be refreshed after processing events with hms event listener enabled
    public long getUpdateTime() {
        return Math.max(this.schemaUpdateTime, this.eventUpdateTime);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        estimatedRowCount = -1;
    }

    @Override
    public List<Long> getChunkSizes() {
        HiveMetaStoreCache.HivePartitionValues partitionValues = StatisticsUtil.getPartitionValuesForTable(this);
        List<HiveMetaStoreCache.FileCacheValue> filesByPartitions
                = StatisticsUtil.getFilesForPartitions(this, partitionValues, 0);
        List<Long> result = Lists.newArrayList();
        for (HiveMetaStoreCache.FileCacheValue files : filesByPartitions) {
            for (HiveMetaStoreCache.HiveFileStatus file : files.getFiles()) {
                result.add(file.getLength());
            }
        }
        return result;
    }

    @Override
    public long getDataSize(boolean singleReplica) {
        long totalSize = StatisticsUtil.getTotalSizeFromHMS(this);
        // Usually, we can get total size from HMS parameter.
        if (totalSize > 0) {
            return totalSize;
        }
        // If not found the size in HMS, calculate it by sum all files' size in table.
        List<Long> chunkSizes = getChunkSizes();
        long total = 0;
        for (long size : chunkSizes) {
            total += size;
        }
        return total;
    }

    @Override
    public boolean isDistributionColumn(String columnName) {
        return getRemoteTable().getSd().getBucketCols().stream().map(String::toLowerCase)
                .collect(Collectors.toSet()).contains(columnName.toLowerCase());
    }

    @Override
    public Set<String> getDistributionColumnNames() {
        return getRemoteTable().getSd().getBucketCols().stream().map(String::toLowerCase)
                .collect(Collectors.toSet());
    }

    @Override
    public PartitionType getPartitionType() {
        return getPartitionColumns().size() > 0 ? PartitionType.LIST : PartitionType.UNPARTITIONED;
    }

    @Override
    public Set<String> getPartitionColumnNames() {
        return getPartitionColumns().stream()
                .map(c -> c.getName().toLowerCase()).collect(Collectors.toSet());
    }

    @Override
    public Map<Long, PartitionItem> getPartitionItems() {
        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) getCatalog());
        HiveMetaStoreCache.HivePartitionValues hivePartitionValues = cache.getPartitionValues(
                getDbName(), getName(), getPartitionColumnTypes());

        return hivePartitionValues.getIdToPartitionItem().entrySet().stream()
                .filter(entry -> !entry.getValue().isDefaultPartition())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public String getPartitionName(long partitionId) throws AnalysisException {
        Map<String, Long> partitionNameToIdMap = getHivePartitionValues().getPartitionNameToIdMap();
        for (Entry<String, Long> entry : partitionNameToIdMap.entrySet()) {
            if (entry.getValue().equals(partitionId)) {
                return entry.getKey();
            }
        }
        throw new AnalysisException("can not find partition,  partitionId: " + partitionId);
    }

    private HiveMetaStoreCache.HivePartitionValues getHivePartitionValues() {
        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) getCatalog());
        return cache.getPartitionValues(
                getDbName(), getName(), getPartitionColumnTypes());
    }

    @Override
    public MTMVSnapshotIf getPartitionSnapshot(long partitionId) throws AnalysisException {
        long partitionLastModifyTime = getPartitionLastModifyTime(partitionId);
        return new MTMVTimestampSnapshot(partitionLastModifyTime);
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot() throws AnalysisException {
        if (getPartitionType() == PartitionType.UNPARTITIONED) {
            return new MTMVMaxTimestampSnapshot(-1L, getLastDdlTime());
        }
        long partitionId = 0L;
        long maxVersionTime = 0L;
        long visibleVersionTime;
        for (Entry<Long, PartitionItem> entry : getPartitionItems().entrySet()) {
            visibleVersionTime = getPartitionLastModifyTime(entry.getKey());
            if (visibleVersionTime > maxVersionTime) {
                maxVersionTime = visibleVersionTime;
                partitionId = entry.getKey();
            }
        }
        return new MTMVMaxTimestampSnapshot(partitionId, maxVersionTime);
    }

    private long getPartitionLastModifyTime(long partitionId) throws AnalysisException {
        return getPartitionById(partitionId).getLastModifiedTime();
    }

    private HivePartition getPartitionById(long partitionId) throws AnalysisException {
        PartitionItem item = getPartitionItems().get(partitionId);
        List<List<String>> partitionValuesList = transferPartitionItemToPartitionValues(item);
        List<HivePartition> partitions = getPartitionsByPartitionValues(partitionValuesList);
        if (partitions.size() != 1) {
            throw new AnalysisException("partition not normal, size: " + partitions.size());
        }
        return partitions.get(0);
    }

    private List<HivePartition> getPartitionsByPartitionValues(List<List<String>> partitionValuesList) {
        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) getCatalog());
        return cache.getAllPartitionsWithCache(getDbName(), getName(),
                partitionValuesList);
    }

    private List<List<String>> transferPartitionItemToPartitionValues(PartitionItem item) {
        List<List<String>> partitionValuesList = Lists.newArrayListWithCapacity(1);
        partitionValuesList.add(
                ((ListPartitionItem) item).getItems().get(0).getPartitionValuesAsStringListForHive());
        return partitionValuesList;
    }
}


