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

package org.apache.doris.catalog;

import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Status;
import org.apache.doris.metric.LongCounterMetric;
import org.apache.doris.metric.MetricRepo;

import org.apache.avro.util.MapEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;


public class TabletStatMgrTest {

    private static final Logger LOG = LoggerFactory.getLogger(TabletStatMgrTest.class);
    private static final long TEST_BE_ID1 = 10001L;
    private static final long TEST_BE_ID2 = 10002L;

    @Mock private MarkedCountDownLatch<Long, Object> updateTabletStatsLatch;
    @Mock private LongCounterMetric counterFailed;

    private TabletStatMgr tabletStatMgr;
    private AutoCloseable mockCloseable;

    @BeforeEach
    public void setUp() throws Exception {
        mockCloseable = MockitoAnnotations.openMocks(this);

        tabletStatMgr = new TabletStatMgr();

        Field latchField = TabletStatMgr.class.getDeclaredField("updateTabletStatsLatch");
        latchField.setAccessible(true);
        latchField.set(tabletStatMgr, updateTabletStatsLatch);

        backupMetricRepoState();
    }

    @AfterEach
    public void tearDown() throws Exception {
        restoreMetricRepoState();
        mockCloseable.close();
    }

    private void backupMetricRepoState() throws Exception {
        MetricRepo.isInit = false;
    }

    private void restoreMetricRepoState() throws Exception {
    }

    private void setMetricRepoInitialized(boolean initialized) {
        MetricRepo.isInit = initialized;
    }

    private void setFailedCounter(LongCounterMetric counter) throws Exception {
        Field counterField = MetricRepo.class.getDeclaredField("COUNTER_UPDATE_TABLET_STAT_FAILED");
        counterField.setAccessible(true);
        counterField.set(null, counter);
    }

    @Test
    public void testWaitSuccess() throws Exception {
        Mockito.when(updateTabletStatsLatch.await(600, TimeUnit.SECONDS)).thenReturn(true);
        Mockito.when(updateTabletStatsLatch.getStatus()).thenReturn(Status.OK);
        Mockito.when(updateTabletStatsLatch.getLeftMarks()).thenReturn(Collections.emptyList());
        setMetricRepoInitialized(true);
        setFailedCounter(counterFailed);

        tabletStatMgr.waitForTabletStatUpdate();

        Mockito.verify(counterFailed, Mockito.never()).increase(Mockito.anyLong());
    }

    @Test
    public void testTimeout() throws Exception {
        Mockito.when(updateTabletStatsLatch.await(600, TimeUnit.SECONDS)).thenReturn(false);
        Mockito.when(updateTabletStatsLatch.getCount()).thenReturn(2L);

        List<Map.Entry<Long, Object>> entries = java.util.Collections.singletonList(
            new MapEntry<>(TEST_BE_ID1, "BE1")
        );
        Mockito.when(updateTabletStatsLatch.getLeftMarks())
                .thenReturn(entries);
        Mockito.when(updateTabletStatsLatch.getStatus()).thenReturn(Status.OK);
        setMetricRepoInitialized(true);
        setFailedCounter(counterFailed);

        tabletStatMgr.waitForTabletStatUpdate();

        Mockito.verify(counterFailed).increase(1L);
    }

    @Test
    public void testInterruptedException() throws Exception {
        Mockito.when(updateTabletStatsLatch.await(600, TimeUnit.SECONDS))
                .thenThrow(new InterruptedException("Test interrupt"));
        Mockito.when(updateTabletStatsLatch.getStatus()).thenReturn(Status.TIMEOUT);
        setMetricRepoInitialized(true);
        setFailedCounter(counterFailed);

        tabletStatMgr.waitForTabletStatUpdate();

        Mockito.verify(counterFailed).increase(1L);
    }

    @Test
    public void testErrorStatus() throws Exception {
        Mockito.when(updateTabletStatsLatch.await(600, TimeUnit.SECONDS)).thenReturn(true);
        Mockito.when(updateTabletStatsLatch.getStatus()).thenReturn(Status.TIMEOUT);

        List<Map.Entry<Long, Object>> entries = java.util.Collections.singletonList(
            new MapEntry<>(TEST_BE_ID1, "BE1")
        );
        Mockito.when(updateTabletStatsLatch.getLeftMarks()).thenReturn(entries);
        setMetricRepoInitialized(true);
        setFailedCounter(counterFailed);

        tabletStatMgr.waitForTabletStatUpdate();

        Mockito.verify(counterFailed).increase(1L);
    }

    @Test
    public void testNoUnfinishedBackends() throws Exception {
        Mockito.when(updateTabletStatsLatch.await(600, TimeUnit.SECONDS)).thenReturn(false);
        Mockito.when(updateTabletStatsLatch.getLeftMarks()).thenReturn(Collections.emptyList());
        Mockito.when(updateTabletStatsLatch.getStatus()).thenReturn(Status.OK);
        setMetricRepoInitialized(true);
        setFailedCounter(counterFailed);

        tabletStatMgr.waitForTabletStatUpdate();

        Mockito.verify(counterFailed).increase(1L);
    }
}
