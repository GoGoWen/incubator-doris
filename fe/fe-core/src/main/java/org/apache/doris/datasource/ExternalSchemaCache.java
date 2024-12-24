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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.metric.GaugeMetric;
import org.apache.doris.metric.Metric;
import org.apache.doris.metric.MetricLabel;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.BDPAuthContext;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;

// The schema cache for external table
public class ExternalSchemaCache {
    private static final Logger LOG = LogManager.getLogger(ExternalSchemaCache.class);
    private final ExternalCatalog catalog;

    private LoadingCache<SchemaCacheKey, Optional<SchemaCacheValue>> schemaCache;

    public ExternalSchemaCache(ExternalCatalog catalog, ExecutorService executor) {
        this.catalog = catalog;
        init(executor);
        initMetrics();
    }

    private void init(ExecutorService executor) {
        CacheFactory schemaCacheFactory = new CacheFactory(
                OptionalLong.of(Config.external_schema_cache_expire_time_minutes_after_write * 60),
                OptionalLong.of(Config.external_cache_expire_time_minutes_after_access * 60),
                Config.max_external_schema_cache_num,
                false,
                null);
        schemaCache = schemaCacheFactory.buildCache(key -> loadSchema(key), null, executor);
    }

    private void initMetrics() {
        // schema cache
        GaugeMetric<Long> schemaCacheGauge = new GaugeMetric<Long>("external_schema_cache",
                Metric.MetricUnit.NOUNIT, "external schema cache number") {
            @Override
            public Long getValue() {
                return schemaCache.estimatedSize();
            }
        };
        schemaCacheGauge.addLabel(new MetricLabel("catalog", catalog.getName()));
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(schemaCacheGauge);
    }

    private Optional<SchemaCacheValue> loadSchema(SchemaCacheKey key) {
        // reload sync if connect_context is null
        if (catalog instanceof HMSExternalCatalog) {
            Preconditions.checkNotNull(BDPAuthContext.get(), "bdp auth info cannot be null");
            DatabaseIf db = catalog.getDbNullable(key.dbName);
            if (db != null) {
                TableIf table = db.getTableNullable(key.tblName);
                if (table != null && table instanceof HMSExternalTable) {
                    if (((HMSExternalTable) table).isViewBased()) {
                        key.fromView = true;
                    }
                }
            }
        }
        Optional<SchemaCacheValue> schema = catalog.getSchema(key.dbName, key.tblName);
        if (LOG.isDebugEnabled()) {
            LOG.debug("load schema for {} in catalog {}", key, catalog.getName());
        }
        return schema;
    }

    public Optional<SchemaCacheValue> getSchemaValueFromView(String dbName, String tblName) {
        if (catalog instanceof HMSExternalCatalog) {
            Preconditions.checkNotNull(BDPAuthContext.get(), "bdp auth info cannot be null");
            SchemaCacheKey key = new SchemaCacheKey(BDPAuthContext.get().getHadoopUserName(), dbName, tblName,
                          true);
            return schemaCache.get(key);
        } else {
            return schemaCache.get(new SchemaCacheKey("", dbName, tblName));
        }
    }

    public Optional<SchemaCacheValue> getSchemaValue(String dbName, String tblName) {
        if (catalog instanceof HMSExternalCatalog) {
            Preconditions.checkNotNull(BDPAuthContext.get(), "bdp auth info cannot be null");
            SchemaCacheKey key = new SchemaCacheKey(BDPAuthContext.get().getHadoopUserName(), dbName, tblName);
            return schemaCache.get(key);
        } else {
            return schemaCache.get(new SchemaCacheKey("", dbName, tblName));
        }
    }

    public void addSchemaForTest(String dbName, String tblName, ImmutableList<Column> schema) {
        if (catalog instanceof HMSExternalCatalog) {
            Preconditions.checkNotNull(BDPAuthContext.get(), "bdp auth info cannot be null");
        }
        SchemaCacheKey key = new SchemaCacheKey(catalog instanceof HMSExternalCatalog
                ? BDPAuthContext.get().getHadoopUserName() : "", dbName, tblName);
        schemaCache.put(key, Optional.of(new SchemaCacheValue(schema)));
    }

    public void invalidateTableCache(String dbName, String tblName) {
        Set<SchemaCacheKey> keys = schemaCache.asMap().keySet();
        for (SchemaCacheKey key : keys) {
            if (key.dbName.equals(dbName) && key.tblName.equals(tblName)) {
                schemaCache.invalidate(key);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalid schema cache for {}.{} in catalog {}", dbName, tblName, catalog.getName());
        }
    }

    public void invalidateDbCache(String dbName) {
        long start = System.currentTimeMillis();
        Set<SchemaCacheKey> keys = schemaCache.asMap().keySet();
        for (SchemaCacheKey key : keys) {
            if (key.dbName.equals(dbName)) {
                schemaCache.invalidate(key);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalid schema cache for db {} in catalog {} cost: {} ms", dbName, catalog.getName(),
                    (System.currentTimeMillis() - start));
        }
    }

    public void invalidateAll() {
        schemaCache.invalidateAll();
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalid all schema cache in catalog {}", catalog.getName());
        }
    }

    @Data
    public static class SchemaCacheKey {
        private String hadoopUserName;
        private String dbName;
        private String tblName;
        private boolean fromView = false;

        public SchemaCacheKey(String hadoopUserName, String dbName, String tblName) {
            this.hadoopUserName = hadoopUserName;
            this.dbName = dbName;
            this.tblName = tblName;
        }

        public SchemaCacheKey(String hadoopUserName, String dbName, String tblName, boolean fromView) {
            this.hadoopUserName = hadoopUserName;
            this.dbName = dbName;
            this.tblName = tblName;
            this.fromView = fromView;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof SchemaCacheKey)) {
                return false;
            }
            return hadoopUserName.equals(((SchemaCacheKey) obj).hadoopUserName)
                    && dbName.equals(((SchemaCacheKey) obj).dbName)
                    && tblName.equals(((SchemaCacheKey) obj).tblName)
                    && fromView == ((SchemaCacheKey) obj).fromView;
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tblName, fromView);
        }

        @Override
        public String toString() {
            return "SchemaCacheKey{" + "hadoopUserName='" + hadoopUserName + '\''
                    + ", dbName='" + dbName + '\'' + ", tblName='" + tblName + '\''
                    + ", fromView='" + fromView + '\'' + '}';
        }
    }
}
