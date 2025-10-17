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

package com.starrocks.common.proc;

import com.google.common.collect.Maps;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.system.TabletComputeNodeMapper;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class TabletMappingProcNodeTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private StarOSAgent starOsAgent;
    @Mocked
    private NodeMgr nodeMgr;
    @Mocked
    private SystemInfoService systemInfoService;
    @Mocked
    private TabletComputeNodeMapper tabletComputeNodeMapper;
    @Mocked
    private WarehouseManager warehouseManager;

    private Frontend thisFe;

    @Before
    public void setUp() {
        thisFe = new Frontend();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 1;
                result = globalStateMgr;
            }
        };

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getNodeMgr();
                minTimes = 1;
                result = nodeMgr;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;
            }
        };

        new Expectations(nodeMgr) {
            {
                nodeMgr.getClusterInfo();
                minTimes = 1;
                result = systemInfoService;

                nodeMgr.getMySelf();
                minTimes = 0;
                result = thisFe;
            }
        };
    }

    @Test
    public void testNoTabletMappings() throws Exception {
        new Expectations(systemInfoService) {

            {
                systemInfoService.internalTabletMapper();
                minTimes = 1;
                result = tabletComputeNodeMapper;
            }
        };

        // Check that when there's no tablet mappings this doesn't break.
        new Expectations(tabletComputeNodeMapper) {
            {
                tabletComputeNodeMapper.getTabletMappingCount();
                times = 1;
                result = Maps.newConcurrentMap();

            }
        };
        TabletMappingProcNode proceNode = new TabletMappingProcNode();
        ProcResult res = proceNode.fetchResult();
    }

    @Test
    public void testNoComputeNodes() throws Exception {
        new Expectations(systemInfoService) {

            {
                systemInfoService.internalTabletMapper();
                minTimes = 1;
                result = tabletComputeNodeMapper;
            }
        };

        // Check that when there's no compute nodes this doesn't break.
        ConcurrentMap<Long, AtomicLong> tabletToMappingCount = Maps.newConcurrentMap();
        tabletToMappingCount.put(1L, new AtomicLong(10));

        new Expectations(tabletComputeNodeMapper) {
            {
                tabletComputeNodeMapper.getTabletMappingCount();
                times = 1;
                result = tabletToMappingCount;

            }

            {
                tabletComputeNodeMapper.backupComputeNodesForTablet(1L, 0L, 1, thisFe.getResourceIsolationGroup());
                times = 1;
                result = List.of();
            }

        };
        TabletMappingProcNode proceNode = new TabletMappingProcNode();
        ProcResult res = proceNode.fetchResult();
    }

    @Test
    public void testOneComputeNodes() throws Exception {
        new Expectations(systemInfoService) {

            {
                systemInfoService.internalTabletMapper();
                minTimes = 1;
                result = tabletComputeNodeMapper;
            }
        };

        // Check that when there's only one compute node this doesn't break.
        ConcurrentMap<Long, AtomicLong> tabletToMappingCount = Maps.newConcurrentMap();
        tabletToMappingCount.put(1L, new AtomicLong(10));

        new Expectations(tabletComputeNodeMapper) {
            {
                tabletComputeNodeMapper.getTabletMappingCount();
                times = 1;
                result = tabletToMappingCount;

            }

            {
                tabletComputeNodeMapper.backupComputeNodesForTablet(1L, 0L, 1, thisFe.getResourceIsolationGroup());
                times = 1;
                result = List.of(100L);
            }
        };
        TabletMappingProcNode proceNode = new TabletMappingProcNode();
        ProcResult res = proceNode.fetchResult();
    }

    @Test
    public void testNormal() throws Exception {
        new Expectations(systemInfoService) {

            {
                systemInfoService.internalTabletMapper();
                minTimes = 1;
                result = tabletComputeNodeMapper;
            }
        };

        // Check that when there's multiple tablets and multiple CN this doesn't break
        ConcurrentMap<Long, AtomicLong> tabletToMappingCount = Maps.newConcurrentMap();
        tabletToMappingCount.put(1L, new AtomicLong(10));
        tabletToMappingCount.put(2L, new AtomicLong(20));

        new Expectations(tabletComputeNodeMapper) {
            {
                tabletComputeNodeMapper.getTabletMappingCount();
                times = 1;
                result = tabletToMappingCount;

            }

            {
                tabletComputeNodeMapper.backupComputeNodesForTablet(1L, 0L, 1, thisFe.getResourceIsolationGroup());
                times = 1;
                result = List.of(100L);
            }

            {
                tabletComputeNodeMapper.backupComputeNodesForTablet(2L, 0L, 1, thisFe.getResourceIsolationGroup());
                times = 1;
                result = List.of(200L);
            }
        };
        TabletMappingProcNode proceNode = new TabletMappingProcNode();
        ProcResult res = proceNode.fetchResult();

        Assert.assertEquals(List.of("TabletId", "BackupRequestCount", "PrimaryCnOwner", "SecondaryCnOwner"),
                res.getColumnNames());
        Assert.assertEquals(List.of(List.of("1", "10", "0", "100"), List.of("2", "20", "0", "200")), res.getRows());
    }
}