// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.metric;

import com.starrocks.catalog.Table;
import com.starrocks.clone.TabletSchedCtx;
import com.starrocks.clone.TabletScheduler;
import com.starrocks.clone.TabletSchedulerStat;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.http.rest.MetricsAction;
import com.starrocks.journal.Journal;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.staros.StarOSBDBJEJournalSystem;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Set;

public class MetricRepoTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        // init brpc so brpc metrics can be collected
        BrpcProxy.getBackendService(new TNetworkAddress("127.0.0.1", 12345));
        PlanTestBase.beforeClass();

        starRocksAssert.withDatabase("test_metric");
    }

    @AfterAll
    public static void afterClass() {
        PlanTestBase.afterClass();
        try {
            starRocksAssert.dropDatabase("test_metric");
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testGetMetric() throws Exception {
        starRocksAssert.useDatabase("test_metric")
                .withTable("create table t1 (c1 int, c2 string)" +
                        " distributed by hash(c1) " +
                        " properties('replication_num'='1') ");
        Table t1 = starRocksAssert.getTable("test_metric", "t1");

        // update metric
        TableMetricsEntity entity = TableMetricsRegistry.getInstance().getMetricsEntity(t1.getId());
        entity.counterScanBytesTotal.increase(1024L);

        // verify metric
        JsonMetricVisitor visitor = new JsonMetricVisitor("m");
        MetricsAction.RequestParams params = new MetricsAction.RequestParams(true, true, true, true);
        MetricRepo.getMetric(visitor, params);
        String json = visitor.build();
        Assertions.assertTrue(StringUtils.isNotEmpty(json));
        Assertions.assertTrue(json.contains("test_metric"));
        Assertions.assertTrue(json.contains("brpc_pool_numactive"));
    }

    @Test
    public void testJournalAndImageMetricsExposure() {
        // same prefix MetricsAction uses, so the asserted names are the ones operators scrape
        MetricVisitor visitor = new PrometheusMetricVisitor("starrocks_fe");
        MetricsAction.RequestParams params = new MetricsAction.RequestParams(true, true, true, true);
        MetricRepo.getMetric(visitor, params);
        String output = visitor.build();

        Assertions.assertTrue(output.contains("starrocks_fe_edit_log{type=\"current\""), output);
        // StarMgr shares the bdbje environment in shared-data mode but cleans up independently, so
        // it gets its own series rather than being folded into the one above
        Assertions.assertTrue(output.contains("starrocks_fe_edit_log{type=\"starmgr_current\""), output);
        Assertions.assertTrue(output.contains("starrocks_fe_image_write{type=\"success\""), output);
        Assertions.assertTrue(output.contains("starrocks_fe_image_write{type=\"failed\""), output);
        Assertions.assertTrue(output.contains("starrocks_fe_image_push{type=\"success\""), output);
        Assertions.assertTrue(output.contains("starrocks_fe_image_push{type=\"failed\""), output);

        // The retained-journal level is a gauge; image counts stay counters. Only one TYPE line is
        // emitted per metric name (PrometheusMetricVisitor dedupes it), so declaring the retained
        // level as a counter would make every cleanup look like a counter reset to Prometheus.
        Assertions.assertTrue(output.contains("# TYPE starrocks_fe_edit_log gauge"), output);
        Assertions.assertTrue(output.contains("# TYPE starrocks_fe_image_write counter"), output);
        Assertions.assertTrue(output.contains("# TYPE starrocks_fe_image_push counter"), output);
    }

    @Test
    public void testGetRetainedJournalCount() {
        // journals 101..200 are still on disk -> 100 retained
        Assertions.assertEquals(100L, MetricRepo.getRetainedJournalCount(journalOf(List.of(101L), 200L)));
        // a single journal written into a freshly rolled database
        Assertions.assertEquals(1L, MetricRepo.getRetainedJournalCount(journalOf(List.of(101L), 101L)));
        // several databases retained: counted from the oldest surviving one
        Assertions.assertEquals(300L, MetricRepo.getRetainedJournalCount(
                journalOf(List.of(1L, 101L, 201L), 300L)));

        // nothing retained / bdb environment closing / journal not open yet
        Assertions.assertEquals(0L, MetricRepo.getRetainedJournalCount(journalOf(List.of(), 200L)));
        Assertions.assertEquals(0L, MetricRepo.getRetainedJournalCount(journalOf(null, 200L)));
        Assertions.assertEquals(0L, MetricRepo.getRetainedJournalCount(null));
        // getMaxJournalId() returns -1 when the environment cannot be read; must not go negative
        Assertions.assertEquals(0L, MetricRepo.getRetainedJournalCount(journalOf(List.of(101L), -1L)));
    }

    /**
     * Shared-nothing mode never builds the StarMgr journal system, so the gauge must read 0 instead
     * of throwing on a null journalSystem and taking the whole /metrics endpoint down with it.
     */
    @Test
    public void testGetRetainedStarMgrJournalCountWithoutStarMgr() {
        Assertions.assertNull(StarMgrServer.getServingState().getJournalSystem());
        Assertions.assertEquals(0L, MetricRepo.getRetainedStarMgrJournalCount());
    }

    /**
     * In shared-data mode the StarMgr journal lives in the same bdbje environment under the
     * "starmgr_" prefix, and its retained count has to be reported independently of the
     * GlobalStateMgr one.
     */
    @Test
    public void testGetRetainedStarMgrJournalCountInSharedData() {
        // built before the stubbing calls below: creating a mock inside when(...) is nested
        // stubbing and Mockito rejects it
        Journal starMgrJournal = journalOf(List.of(201L), 500L);
        StarOSBDBJEJournalSystem journalSystem = Mockito.mock(StarOSBDBJEJournalSystem.class);
        Mockito.when(journalSystem.getJournal()).thenReturn(starMgrJournal);
        StarMgrServer starMgrServer = Mockito.mock(StarMgrServer.class);
        Mockito.when(starMgrServer.getJournalSystem()).thenReturn(journalSystem);

        try (MockedStatic<StarMgrServer> mocked = Mockito.mockStatic(StarMgrServer.class)) {
            mocked.when(StarMgrServer::getServingState).thenReturn(starMgrServer);

            Assertions.assertEquals(300L, MetricRepo.getRetainedStarMgrJournalCount());
        }
    }

    private static Journal journalOf(List<Long> databaseNames, long maxJournalId) {
        Journal journal = Mockito.mock(Journal.class);
        Mockito.when(journal.getDatabaseNames()).thenReturn(databaseNames);
        Mockito.when(journal.getMaxJournalId()).thenReturn(maxJournalId);
        return journal;
    }

    @Test
    public void testLeaderAwarenessMetric() {
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().isLeader());

        List<Metric> metrics = MetricRepo.getMetricsByName("job");
        MetricVisitor visitor = new PrometheusMetricVisitor("");
        for (Metric m : metrics) {
            visitor.visit(m);
        }
        // _job{is_leader="true", job="load", type="INSERT", state="UNKNOWN"} 0
        // _job{is_leader="true", job="load", type="INSERT", state="PENDING"} 0
        // _job{is_leader="true", job="load", type="INSERT", state="ETL"} 0
        // _job{is_leader="true", job="load", type="INSERT", state="LOADING"} 0
        // _job{is_leader="true", job="load", type="INSERT", state="COMMITTED"} 0
        // _job{is_leader="true", job="load", type="INSERT", state="FINISHED"} 0
        // _job{is_leader="true", job="load", type="INSERT", state="CANCELLED"} 0
        // _job{is_leader="true", job="load", type="INSERT", state="QUEUEING"} 0
        // _job{is_leader="true", job="load", type="BROKER", state="UNKNOWN"} 0
        // _job{is_leader="true", job="load", type="BROKER", state="PENDING"} 0
        // _job{is_leader="true", job="load", type="BROKER", state="ETL"} 0
        // _job{is_leader="true", job="load", type="BROKER", state="LOADING"} 0
        // _job{is_leader="true", job="load", type="BROKER", state="COMMITTED"} 0
        // _job{is_leader="true", job="load", type="BROKER", state="FINISHED"} 0
        // _job{is_leader="true", job="load", type="BROKER", state="CANCELLED"} 0
        // _job{is_leader="true", job="load", type="BROKER", state="QUEUEING"} 0
        // _job{is_leader="true", job="load", type="SPARK", state="UNKNOWN"} 0
        // _job{is_leader="true", job="load", type="SPARK", state="PENDING"} 0
        // _job{is_leader="true", job="load", type="SPARK", state="ETL"} 0
        // _job{is_leader="true", job="load", type="SPARK", state="LOADING"} 0
        // _job{is_leader="true", job="load", type="SPARK", state="COMMITTED"} 0
        // _job{is_leader="true", job="load", type="SPARK", state="FINISHED"} 0
        // _job{is_leader="true", job="load", type="SPARK", state="CANCELLED"} 0
        // _job{is_leader="true", job="load", type="SPARK", state="QUEUEING"} 0
        // _job{is_leader="true", job="alter", type="ROLLUP", state="running"} 0
        // _job{is_leader="true", job="alter", type="SCHEMA_CHANGE", state="running"} 0
        String output = visitor.build();
        String [] lines = output.split("\n");
        for (String line : lines) {
            if (line.startsWith("#")) {
                continue;
            }
            Assertions.assertTrue(line.contains("is_leader=\"true\""), line);
        }
    }

    @Test
    public void testRoutineLoadJobMetrics() {
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().isLeader());
        List<Metric> metrics = MetricRepo.getMetricsByName("routine_load_jobs");
        MetricVisitor visitor = new PrometheusMetricVisitor("ut");
        for (Metric m : metrics) {
            visitor.visit(m);
        }
        // ut_routine_load_jobs{is_leader="true", state="NEED_SCHEDULE"} 0
        // ut_routine_load_jobs{is_leader="true", state="RUNNING"} 0
        // ut_routine_load_jobs{is_leader="true", state="PAUSED"} 0
        // ut_routine_load_jobs{is_leader="true", state="STOPPED"} 0
        // ut_routine_load_jobs{is_leader="true", state="CANCELLED"} 0
        // ut_routine_load_jobs{is_leader="true", state="UNSTABLE"} 0
        String output = visitor.build();
        String[] lines = output.split("\n");
        for (String line : lines) {
            if (line.startsWith("#")) {
                continue;
            }
            Assertions.assertTrue(line.contains("is_leader=\"true\""), line);
        }
    }

    @Test
    public void testCloneMetrics() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Assertions.assertTrue(globalStateMgr.isLeader());

        TabletScheduler tabletScheduler = globalStateMgr.getTabletScheduler();
        TabletSchedulerStat stat = tabletScheduler.getStat();
        stat.counterCloneTask.incrementAndGet(); // 1
        stat.counterCloneTaskSucceeded.incrementAndGet(); // 1
        stat.counterCloneTaskInterNodeCopyBytes.addAndGet(100L);
        stat.counterCloneTaskInterNodeCopyDurationMs.addAndGet(10L);
        stat.counterCloneTaskIntraNodeCopyBytes.addAndGet(101L);
        stat.counterCloneTaskIntraNodeCopyDurationMs.addAndGet(11L);

        TabletSchedCtx ctx1 = new TabletSchedCtx(TabletSchedCtx.Type.BALANCE, 1L, 2L, 3L, 4L, 1001L, System.currentTimeMillis());
        Deencapsulation.invoke(tabletScheduler, "addToPendingTablets", ctx1);
        TabletSchedCtx ctx2 = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR, 1L, 2L, 3L, 4L, 1002L, System.currentTimeMillis());
        Deencapsulation.invoke(tabletScheduler, "addToRunningTablets", ctx2);
        Set<Long> allTabletIds = Deencapsulation.getField(tabletScheduler, "allTabletIds");
        allTabletIds.add(1001L);
        allTabletIds.add(1002L);

        // check
        List<Metric> scheduledTabletNum = MetricRepo.getMetricsByName("scheduled_tablet_num");
        Assertions.assertEquals(1, scheduledTabletNum.size());
        Assertions.assertEquals(2L, (Long) scheduledTabletNum.get(0).getValue());

        List<Metric> scheduledPendingTabletNums = MetricRepo.getMetricsByName("scheduled_pending_tablet_num");
        Assertions.assertEquals(2, scheduledPendingTabletNums.size());
        for (Metric metric : scheduledPendingTabletNums) {
            // label 0 is is_leader
            MetricLabel label0 = (MetricLabel) metric.getLabels().get(0);
            Assertions.assertEquals("is_leader", label0.getKey());
            Assertions.assertEquals("true", label0.getValue());

            MetricLabel label1 = (MetricLabel) metric.getLabels().get(1);
            String type = label1.getValue();
            if (type.equals("REPAIR")) {
                Assertions.assertEquals(0L, metric.getValue());
            } else if (type.equals("BALANCE")) {
                Assertions.assertEquals(1L, metric.getValue());
            } else {
                Assertions.fail("Unknown type: " + type);
            }
        }

        List<Metric> scheduledRunningTabletNums = MetricRepo.getMetricsByName("scheduled_running_tablet_num");
        Assertions.assertEquals(2, scheduledRunningTabletNums.size());
        for (Metric metric : scheduledRunningTabletNums) {
            // label 0 is is_leader
            MetricLabel label = (MetricLabel) metric.getLabels().get(1);
            String type = label.getValue();
            if (type.equals("REPAIR")) {
                Assertions.assertEquals(1L, metric.getValue());
            } else if (type.equals("BALANCE")) {
                Assertions.assertEquals(0L, metric.getValue());
            } else {
                Assertions.fail("Unknown type: " + type);
            }
        }

        List<Metric> cloneTaskTotal = MetricRepo.getMetricsByName("clone_task_total");
        Assertions.assertEquals(1, cloneTaskTotal.size());
        Assertions.assertEquals(1L, (Long) cloneTaskTotal.get(0).getValue());

        List<Metric> cloneTaskSuccess = MetricRepo.getMetricsByName("clone_task_success");
        Assertions.assertEquals(1, cloneTaskSuccess.size());
        Assertions.assertEquals(1L, (Long) cloneTaskSuccess.get(0).getValue());

        List<Metric> cloneTaskCopyBytes = MetricRepo.getMetricsByName("clone_task_copy_bytes");
        Assertions.assertEquals(2, cloneTaskCopyBytes.size());
        for (Metric metric : cloneTaskCopyBytes) {
            // label 0 is is_leader
            MetricLabel label0 = (MetricLabel) metric.getLabels().get(0);
            Assertions.assertEquals("is_leader", label0.getKey());
            Assertions.assertEquals("true", label0.getValue());

            MetricLabel label1 = (MetricLabel) metric.getLabels().get(1);
            String type = label1.getValue();
            if (type.equals("INTER_NODE")) {
                Assertions.assertEquals(100L, metric.getValue());
            } else if (type.equals("INTRA_NODE")) {
                Assertions.assertEquals(101L, metric.getValue());
            } else {
                Assertions.fail("Unknown type: " + type);
            }
        }

        List<Metric> cloneTaskCopyDurationMs = MetricRepo.getMetricsByName("clone_task_copy_duration_ms");
        Assertions.assertEquals(2, cloneTaskCopyDurationMs.size());
        for (Metric metric : cloneTaskCopyDurationMs) {
            // label 0 is is_leader
            MetricLabel label0 = (MetricLabel) metric.getLabels().get(0);
            Assertions.assertEquals("is_leader", label0.getKey());
            Assertions.assertEquals("true", label0.getValue());

            MetricLabel label1 = (MetricLabel) metric.getLabels().get(1);
            String type = label1.getValue();
            if (type.equals("INTER_NODE")) {
                Assertions.assertEquals(10L, metric.getValue());
            } else if (type.equals("INTRA_NODE")) {
                Assertions.assertEquals(11L, metric.getValue());
            } else {
                Assertions.fail("Unknown type: " + type);
            }
        }
    }

    @Test
    public void testRoutineLoadLagTimeMetricsCollection() {
        // Test that routine load lag time metrics are collected when config is enabled
        boolean originalConfigValue = Config.enable_routine_load_lag_time_metrics;
        try {
            // Test case 1: Config enabled - should collect metrics
            Config.enable_routine_load_lag_time_metrics = true;
            
            JsonMetricVisitor visitor = new JsonMetricVisitor("test");
            MetricsAction.RequestParams params = new MetricsAction.RequestParams(true, true, true, true);
            
            // This should execute line 914 and call RoutineLoadLagTimeMetricMgr.getInstance().collectRoutineLoadLagTimeMetrics(visitor)
            String result = MetricRepo.getMetric(visitor, params);
            
            // Verify that the method completed successfully (no exceptions thrown)
            Assert.assertNotNull(result);
            
            // Test case 2: Config disabled - should skip metrics collection
            Config.enable_routine_load_lag_time_metrics = false;
            
            JsonMetricVisitor visitor2 = new JsonMetricVisitor("test2");
            String result2 = MetricRepo.getMetric(visitor2, params);
            
            // Verify that the method completed successfully even when config is disabled
            Assert.assertNotNull(result2);
            
        } finally {
            // Restore original config value
            Config.enable_routine_load_lag_time_metrics = originalConfigValue;
        }
    }

}
