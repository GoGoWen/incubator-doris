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

package org.apache.doris.statistics;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class StatisticCleanerTest {

    private static final Logger LOG = LoggerFactory.getLogger(StatisticCleanerTest.class);
    @Mock private Env env;
    @Mock private AnalysisManager analysisManager;
    @Mock private InternalCatalog internalCatalog;
    @Mock private TableIf table;

    private StatisticsCleaner statsCleaner;

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        statsCleaner = new StatisticsCleaner();
        Mockito.when(env.getAnalysisManager()).thenReturn(analysisManager);
    }

    @Test
    public void shouldKeepStatsForExistingTable() throws Exception {
        Set<Long> tableIds = new HashSet<>(Arrays.asList(101L));
        TableStatsMeta validStats = createStatsMeta(101L, "default", "db1", "table1");

        Mockito.when(analysisManager.getIdToTblStatsKeys()).thenReturn(tableIds);
        Mockito.when(analysisManager.findTableStatsStatus(101L)).thenReturn(validStats);
        Mockito.when(table.getId()).thenReturn(101L);

        try (org.mockito.MockedStatic<Env> utilEnvMock = Mockito.mockStatic(Env.class)) {
            utilEnvMock.when(() -> Env.getCurrentEnv()).thenReturn(env);
            utilEnvMock.when(() -> Env.getCurrentInternalCatalog()).thenReturn(internalCatalog);
            try (org.mockito.MockedStatic<StatisticsUtil> utilMock = Mockito.mockStatic(StatisticsUtil.class)) {
                utilMock.when(() -> StatisticsUtil.findTable("default", "db1", "table1"))
                        .thenReturn(table);
                invokeClearTableStats();
            }
        }

        Mockito.verify(analysisManager, Mockito.never()).removeTableStats(Mockito.anyLong());
    }

    private TableStatsMeta createStatsMeta(long id, String catalog, String db, String table) {
        TableStatsMeta meta = new TableStatsMeta(id, catalog, db, table);
        return meta;
    }

    private void invokeClearTableStats() throws Exception {
        try {
            Method method = StatisticsCleaner.class.getDeclaredMethod("clearTableStats");
            method.setAccessible(true);
            method.invoke(statsCleaner);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("clearTableStats method not found in StatisticsCleaner", e);
        } catch (Exception e) {
            LOG.error("Error invoking clearTableStats", e);
            throw e;
        }
    }

}
