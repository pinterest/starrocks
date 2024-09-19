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
package com.starrocks.qe;

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.UserException;
import com.starrocks.lake.qe.scheduler.DefaultSharedDataWorkerProvider;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.system.TabletComputeNodeMapper;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import oshi.SystemInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_ID;
import static org.junit.jupiter.api.Assertions.*;

public class CacheSelectBackendSelectorTest {

    @Before
    public void setUp() {
    }

    private OlapScanNode newOlapScanNode(int id, int numBuckets) {
        // copy from fe/fe-core/src/test/java/com/starrocks/qe/ColocatedBackendSelectorTest.java
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        OlapTable table = new OlapTable();
        table.setDefaultDistributionInfo(new HashDistributionInfo(numBuckets, Collections.emptyList()));
        desc.setTable(table);
        return new OlapScanNode(new PlanNodeId(id), desc, "OlapScanNode");
    }

    /**
     * Generate a list of ScanRangeLocations, contains n element for bucketNum
     *
     * @param n         number of ScanRangeLocations
     * @param bucketNum number of buckets
     * @return lists of ScanRangeLocations
     */
    private List<TScanRangeLocations> generateScanRangeLocations(Map<Long, ComputeNode> nodes, int n, int bucketNum,
                                                                 boolean isInternalScan) {
        List<TScanRangeLocations> locations = Lists.newArrayList();
        int currentBucketIndex = 0;
        Iterator<Map.Entry<Long, ComputeNode>> iterator = nodes.entrySet().iterator();
        for (int i = 0; i < n; ++i) {
            if (!iterator.hasNext()) {
                iterator = nodes.entrySet().iterator();
            }
            TScanRange range = new TScanRange();
            if (isInternalScan) {
                TInternalScanRange internalRange = new TInternalScanRange();
                internalRange.setBucket_sequence(currentBucketIndex);
                internalRange.setRow_count(1);
                range.setInternal_scan_range(internalRange);
            }

            TScanRangeLocations loc = new TScanRangeLocations();
            loc.setScan_range(range);

            TScanRangeLocation location = new TScanRangeLocation();
            ComputeNode node = iterator.next().getValue();
            location.setBackend_id(node.getId());
            location.setServer(node.getAddress());
            loc.addToLocations(location);

            locations.add(loc);
            currentBucketIndex = (currentBucketIndex + 1) % bucketNum;
        }
        return locations;
    }

    @Test
    public void testSelectBackendKnownTabletIdAndInternalMapping(@Mocked WarehouseManager warehouseManager,
                                                                 @Mocked SystemInfoService systemInfoService) {
        ScanNode scanNode = newOlapScanNode(1, 1);
        Map<Long, ComputeNode> nodes = new HashMap<>();
        List<Long> nodeIds = new ArrayList<>();
        int totalCnCount = 100;
        TabletComputeNodeMapper tabletComputeNodeMapper = new TabletComputeNodeMapper();
        for (long computeNodeId = 0; computeNodeId < totalCnCount; computeNodeId++) {
            ComputeNode cn = new ComputeNode(computeNodeId, "whatever", 100);
            cn.setAlive(true);
            if (computeNodeId % 3 == 0) {
                cn.setResourceIsolationGroup("group1");
                tabletComputeNodeMapper.addComputeNode(computeNodeId, "group1");
            } else if (computeNodeId % 3 == 1) {
                cn.setResourceIsolationGroup("group2");
                tabletComputeNodeMapper.addComputeNode(computeNodeId, "group2");
            } else {
                cn.setResourceIsolationGroup("group3");
                tabletComputeNodeMapper.addComputeNode(computeNodeId, "group3");

            }
            nodes.put(computeNodeId, cn);
            nodeIds.add(computeNodeId);
        }
        new Expectations() {{
            systemInfoService.shouldUseInternalTabletToCnMapper();
            times = 2; // Once per resource isolation group
            result = true;

            systemInfoService.internalTabletMapper();
            result = tabletComputeNodeMapper;
        }};


        // Internal scans do have tabletIds and should therefore use the internalTabletMapper.
        List<TScanRangeLocations> locations = generateScanRangeLocations(nodes, 1, 1, true);
        // Confirm our assumption that the TScanRangeLocations we used in the test has the 0th CN assigned.
        long givenTabletId = 0L;
        Assert.assertEquals(givenTabletId, locations.get(0).scan_range.internal_scan_range.tablet_id);

        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        CacheSelectComputeNodeSelectionProperties props = new
                CacheSelectComputeNodeSelectionProperties(List.of("group1", "group3"), 3);
        CacheSelectBackendSelector selector = new CacheSelectBackendSelector(
                scanNode, locations, assignment, props, DEFAULT_WAREHOUSE_ID);

        try {
            selector.computeScanRangeAssignment();
        } catch (UserException e) {
            throw new RuntimeException(e);
        } finally {
            List<Long> expectedIds = Stream.concat(
                    tabletComputeNodeMapper.computeNodesForTablet(givenTabletId, props.numReplicasDesired,
                            "group1").stream(),
                    tabletComputeNodeMapper.computeNodesForTablet(givenTabletId, props.numReplicasDesired,
                            "group3").stream())
                    .collect(Collectors.toList());

            Assert.assertEquals(new HashSet<>(expectedIds), selector.getSelectedWorkerIds());

            int givenScanNodeId = scanNode.getId().asInt();
            TScanRangeParams givenRangeParams = new TScanRangeParams(locations.get(0).scan_range);
            FragmentScanRangeAssignment expectedAssignment = new FragmentScanRangeAssignment();
            for (Long id : expectedIds) {
                expectedAssignment.put(id, givenScanNodeId, givenRangeParams);
            }
            Assert.assertEquals(expectedAssignment, assignment);
        }
    }

    @Test
    public void testSelectBackendUnknownTabletId(@Mocked WarehouseManager warehouseManager,
                                                 @Mocked SystemInfoService systemInfoService) {
        ScanNode scanNode = newOlapScanNode(1, 1);
        Map<Long, ComputeNode> nodes = new HashMap<>();
        List<Long> nodeIds = new ArrayList<>();
        int totalCnCount = 100;
        for (long computeNodeId = 0; computeNodeId < totalCnCount; computeNodeId++) {
            ComputeNode cn = new ComputeNode(computeNodeId, "whatever", 100);
            cn.setAlive(true);
            if (computeNodeId % 3 == 0) {
                cn.setResourceIsolationGroup("group1");
            } else if (computeNodeId % 3 == 1) {
                cn.setResourceIsolationGroup("group2");
            } else {
                cn.setResourceIsolationGroup("group3");
            }
            nodes.put(computeNodeId, cn);
            nodeIds.add(computeNodeId);
            long finalComputeNodeId = computeNodeId;
            new Expectations() {{
                systemInfoService.getBackendOrComputeNode(finalComputeNodeId);
                result = cn;
            }};
        }
        new Expectations() {{
            warehouseManager.getAllComputeNodeIds(DEFAULT_WAREHOUSE_ID);
            result = nodeIds;
        }};


        // Non-internal scans don't have tabletIds and should therefore use the workerProviders to get backups.
        List<TScanRangeLocations> locations = generateScanRangeLocations(nodes, 1, 1, false);
        // Confirm our assumption that the TScanRangeLocations we used in the test has the 0th CN assigned.
        Assert.assertEquals(0, locations.get(0).locations.get(0).backend_id);

        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        CacheSelectComputeNodeSelectionProperties props = new
                CacheSelectComputeNodeSelectionProperties(List.of("group1", "group3"), 3);
        CacheSelectBackendSelector selector = new CacheSelectBackendSelector(
                scanNode, locations, assignment, props, DEFAULT_WAREHOUSE_ID);

        try {
            selector.computeScanRangeAssignment();
        } catch (UserException e) {
            throw new RuntimeException(e);
        } finally {
            // For group1, CNs 0, 3, 6 will be assigned
            // For group3, CNs 2, 5, 8 should be assigned.
            Assert.assertEquals(Set.of(0L, 3L, 6L, 2L, 5L, 8L), selector.getSelectedWorkerIds());

            int givenScanNodeId = scanNode.getId().asInt();
            TScanRangeParams givenRangeParams = new TScanRangeParams(locations.get(0).scan_range);
            FragmentScanRangeAssignment expectedAssignment = new FragmentScanRangeAssignment();
            expectedAssignment.put(0L, givenScanNodeId, givenRangeParams);
            expectedAssignment.put(3L, givenScanNodeId, givenRangeParams);
            expectedAssignment.put(6L, givenScanNodeId, givenRangeParams);
            expectedAssignment.put(2L, givenScanNodeId, givenRangeParams);
            expectedAssignment.put(5L, givenScanNodeId, givenRangeParams);
            expectedAssignment.put(8L, givenScanNodeId, givenRangeParams);
            Assert.assertEquals(expectedAssignment, assignment);
        }
    }
}