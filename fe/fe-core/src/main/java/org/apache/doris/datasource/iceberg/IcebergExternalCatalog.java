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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public abstract class IcebergExternalCatalog extends ExternalCatalog {

    private static final Logger LOG = LogManager.getLogger(IcebergExternalCatalog.class);
    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
    public static final String ICEBERG_HLL_COLUMNS = "iceberg.hll.columns";
    public static final String ICEBERG_BITMAP_COLUMNS = "iceberg.bitmap.columns";
    public static final String ICEBERG_REST = "rest";
    public static final String ICEBERG_HMS = "hms";
    public static final String ICEBERG_HADOOP = "hadoop";
    public static final String ICEBERG_GLUE = "glue";
    public static final String ICEBERG_DLF = "dlf";
    protected String icebergCatalogType;
    protected Catalog catalog;
    protected SupportsNamespaces nsCatalog;
    private HashSet<String> hllColumns = new HashSet<>();
    private HashSet<String> bitmapColumns = new HashSet<>();

    public IcebergExternalCatalog(long catalogId, String name, String comment) {
        super(catalogId, name, InitCatalogLog.Type.ICEBERG, comment);
    }

    @Override
    protected void init() {
        nsCatalog = (SupportsNamespaces) catalog;
        initColumnMapping();
        super.init();
    }

    @Override
    protected void prcessInitCatalogLog(InitCatalogLog initCatalogLog) {
        // init hllColumns
        String hllColumnsStr = getProperties().get(ICEBERG_HLL_COLUMNS);
        if (hllColumnsStr != null) {
            initCatalogLog.setHllColumns(hllColumnsStr);
        }
        // init bitmapColumns
        String bitmapColumnsStr = getProperties().get(ICEBERG_BITMAP_COLUMNS);
        if (bitmapColumnsStr != null) {
            initCatalogLog.setBitmapColumns(bitmapColumnsStr);
        }
    }

    public HashSet<String> getHllColumns() {
        return hllColumns;
    }

    public HashSet<String> getBitmapColumns() {
        return bitmapColumns;
    }

    private void initColumnMapping() {
        // init hllColumns
        String hllColumnsStr = getProperties().get(ICEBERG_HLL_COLUMNS);
        if (hllColumns == null) {
            hllColumns = new HashSet<>();
        }
        if (hllColumnsStr != null) {
            String[] columnsHll = hllColumnsStr.split(",");
            hllColumns.addAll(Arrays.asList(columnsHll));
        }
        // init bitmapColumns
        String bitmapColumnsStr = getProperties().get(ICEBERG_BITMAP_COLUMNS);
        if (bitmapColumns == null) {
            bitmapColumns = new HashSet<>();
        }
        if (bitmapColumnsStr != null) {
            String[] columnsBitmap = bitmapColumnsStr.split(",");
            bitmapColumns.addAll(Arrays.asList(columnsBitmap));
        }
    }

    @Override
    public void replayInitCatalog(InitCatalogLog log) {
        super.replayInitCatalog(log);
        // init hllColumns and bitmap columns
        String hllCols = log.getHllColumns();
        if (hllCols != null) {
            if (hllColumns == null) {
                hllColumns = new HashSet<>();
            }
            String[] columnsHll = hllCols.split(",");
            hllColumns.addAll(Arrays.asList(columnsHll));
        }
        // init bitmapColumns
        String bitmapColumnsStr = log.getHllColumns();
        if (bitmapColumnsStr != null) {
            if (bitmapColumns == null) {
                bitmapColumns = new HashSet<>();
            }
            String[] columnsBitmap = bitmapColumnsStr.split(",");
            bitmapColumns.addAll(Arrays.asList(columnsBitmap));
        }
    }

    public Catalog getCatalog() {
        makeSureInitialized();
        return catalog;
    }

    public SupportsNamespaces getNsCatalog() {
        makeSureInitialized();
        return nsCatalog;
    }

    public String getIcebergCatalogType() {
        makeSureInitialized();
        return icebergCatalogType;
    }

    protected List<String> listDatabaseNames() {
        return nsCatalog.listNamespaces().stream()
            .map(e -> e.toString())
            .collect(Collectors.toList());
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        return catalog.tableExists(TableIdentifier.of(dbName, tblName));
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        List<TableIdentifier> tableIdentifiers = catalog.listTables(Namespace.of(dbName));
        return tableIdentifiers.stream().map(TableIdentifier::name).collect(Collectors.toList());
    }

    public org.apache.iceberg.Table getIcebergTable(String dbName, String tblName) {
        makeSureInitialized();
        return Env.getCurrentEnv()
                .getExtMetaCacheMgr()
                .getIcebergMetadataCache()
                .getIcebergTable(catalog, id, dbName, tblName, getProperties());
    }
}
