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
import com.staros.client.StarClientException;
import com.staros.proto.ShardInfo;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.qe.scheduler.DefaultSharedDataWorkerProvider;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.system.TabletComputeNodeMapper;
import com.starrocks.system.WorkerGroupManager;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_ID;
import static org.junit.Assert.assertThrows;

public class CacheSelectBackendSelectorTest {
    private GlobalStateMgr globalStateMgr;

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
    public void testDefaultParams(@Mocked NodeMgr nodeMgr) {
        Frontend fe = new Frontend();
        fe.setResourceIsolationGroup("something");
        new Expectations() {
            {
                nodeMgr.getMySelf();
                result = fe;
                times = 1;
            }
        };
        CacheSelectComputeNodeSelectionProperties props =
                new CacheSelectComputeNodeSelectionProperties(Collections.emptyList(), -1, -1);
        Assert.assertEquals(List.of("something"), props.resourceIsolationGroups);
        Assert.assertEquals(0, props.numReplicasDesired);
        Assert.assertEquals(0, props.numBackupReplicasDesired);
    }

    @Test
    public void testSelectBackendKnownTabletIdAndInternalMapping(@Mocked SystemInfoService systemInfoService,
                                                                 @Mocked WorkerProvider callerWorkerProvider,
                                                                 @Mocked WorkerGroupManager workerGroupManager,
                                                                 @Mocked StarOSAgent starOsAgent) throws StarClientException {
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

        // Internal scans do have tabletIds and should therefore use the internalTabletMapper.
        List<TScanRangeLocations> locations = generateScanRangeLocations(nodes, 1, 1, true);
        // Confirm our assumption that the TScanRangeLocations we used in the test has the 0th CN assigned.
        Long givenTabletId = 0L;
        Assert.assertEquals(givenTabletId.longValue(), locations.get(0).scan_range.internal_scan_range.tablet_id);
        CacheSelectComputeNodeSelectionProperties props =
                new CacheSelectComputeNodeSelectionProperties(List.of("group1", "group3"), 3, 0);
        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        CacheSelectBackendSelector selector =
                new CacheSelectBackendSelector(scanNode, locations, assignment, callerWorkerProvider, props,
                        DEFAULT_WAREHOUSE_ID);

        Long starOsPreferredCnId = -1L;
        ShardInfo mockedShardInfo = ShardInfo.newBuilder().build();
        List<Long> expectedSelections = new ArrayList<>();
        expectedSelections.add(starOsPreferredCnId);
        expectedSelections.addAll(
                tabletComputeNodeMapper.backupComputeNodesForTablet(givenTabletId, starOsPreferredCnId, props.numReplicasDesired,
                        "group1"));
        expectedSelections.add(starOsPreferredCnId);
        expectedSelections.addAll(
                tabletComputeNodeMapper.backupComputeNodesForTablet(givenTabletId, starOsPreferredCnId, props.numReplicasDesired,
                        "group3"));
        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);
        new Expectations(globalStateMgr) {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getStarOSAgent();
                result = starOsAgent;
            }
        };
        new Expectations() {
            {
                workerGroupManager.getWorkerGroup("group1");
                result = 1L;

                workerGroupManager.getWorkerGroup("group3");
                result = 3L;

                starOsAgent.getShardInfo(givenTabletId, 1L);
                result = mockedShardInfo;

                starOsAgent.getShardInfo(givenTabletId, 3L);
                result = mockedShardInfo;

                starOsAgent.getAllNodeIdsByShard(mockedShardInfo, true);
                result = starOsPreferredCnId;

                systemInfoService.internalTabletMapper();
                result = tabletComputeNodeMapper;

                for (Long cnId : expectedSelections) {
                    callerWorkerProvider.selectWorkerUnchecked(cnId);
                    minTimes = 1;
                }

                callerWorkerProvider.setAllowGetAnyWorker(true);
                times = 1;
            }
        };

        try {
            selector.computeScanRangeAssignment();
        } catch (UserException e) {
            System.out.printf("Error: %s%n", e);
            throw new RuntimeException(e);
        } finally {
            int givenScanNodeId = scanNode.getId().asInt();
            TScanRangeParams givenRangeParams = new TScanRangeParams(locations.get(0).scan_range);
            FragmentScanRangeAssignment expectedAssignment = new FragmentScanRangeAssignment();
            expectedSelections.forEach(id -> expectedAssignment.put(id, givenScanNodeId, givenRangeParams));
            Assert.assertEquals(expectedAssignment, assignment);
        }
    }

    @Test
    public void testSelectBackendUnknownTabletId(@Mocked WarehouseManager warehouseManager,
                                                 @Mocked SystemInfoService systemInfoService,
                                                 @Mocked WorkerProvider callerWorkerProvider) {
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
            new Expectations() {
                {
                    systemInfoService.getBackendOrComputeNode(finalComputeNodeId);
                    result = cn;
                }
            };
        }

        // Non-internal scans don't have tabletIds and should therefore use the workerProviders to get backups.
        List<TScanRangeLocations> locations = generateScanRangeLocations(nodes, 1, 1, false);
        // Confirm our assumption that the TScanRangeLocations we used in the test has the 0th CN assigned.
        Assert.assertEquals(0, locations.get(0).locations.get(0).backend_id);

        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        CacheSelectComputeNodeSelectionProperties props =
                new CacheSelectComputeNodeSelectionProperties(List.of("group1", "group3"), 3, 0);
        CacheSelectBackendSelector selector =
                new CacheSelectBackendSelector(scanNode, locations, assignment, callerWorkerProvider, props,
                        DEFAULT_WAREHOUSE_ID);
        new Expectations() {
            {
                warehouseManager.getAllComputeNodeIds(DEFAULT_WAREHOUSE_ID);
                result = nodeIds;

                // For group1, CNs 0, 3, 6 will be assigned
                // For group3, CNs 2, 5, 8 should be assigned.
                for (Long cnId : Set.of(0L, 3L, 6L, 2L, 5L, 8L)) {
                    callerWorkerProvider.selectWorkerUnchecked(cnId);
                    minTimes = 1;
                }

                callerWorkerProvider.setAllowGetAnyWorker(true);
                times = 1;
            }
        };

        try {
            selector.computeScanRangeAssignment();
        } catch (UserException e) {
            throw new RuntimeException(e);
        } finally {
            int givenScanNodeId = scanNode.getId().asInt();
            TScanRangeParams givenRangeParams = new TScanRangeParams(locations.get(0).scan_range);
            FragmentScanRangeAssignment expectedAssignment = new FragmentScanRangeAssignment();
            Stream.of(0L, 3L, 6L, 2L, 5L, 8L).forEach(id -> expectedAssignment.put(id, givenScanNodeId, givenRangeParams));
            Assert.assertEquals(expectedAssignment, assignment);
        }
    }

    @Test
    public void testInsufficientWorkerCountThrowsExceptionNoTablet(@Mocked WarehouseManager warehouseManager,
                                                                   @Mocked SystemInfoService systemInfoService,
                                                                   @Mocked WorkerProvider callerWorkerProvider) {
        ScanNode scanNode = newOlapScanNode(1, 1);
        Map<Long, ComputeNode> nodes = new HashMap<>();
        List<Long> nodeIds = new ArrayList<>();
        {
            ComputeNode cn = new ComputeNode(1L, "whatever", 100);
            cn.setAlive(true);
            cn.setResourceIsolationGroup("group1");
            nodes.put(cn.getId(), cn);
            nodeIds.add(cn.getId());
        }
        {
            ComputeNode cn = new ComputeNode(2L, "whatever", 100);
            cn.setAlive(true);
            cn.setResourceIsolationGroup("group1");
            nodes.put(cn.getId(), cn);
            nodeIds.add(cn.getId());
        }
        {
            ComputeNode cn = new ComputeNode(3L, "whatever", 100);
            cn.setAlive(true);
            cn.setResourceIsolationGroup("group2");
            nodes.put(cn.getId(), cn);
            nodeIds.add(cn.getId());
        }
        for (ComputeNode cn : nodes.values()) {
            new Expectations() {
                {
                    systemInfoService.getBackendOrComputeNode(cn.getId());
                    result = cn;
                }
            };
        }

        // Non-internal scans don't have tabletIds and should therefore use the workerProviders to get backups.
        List<TScanRangeLocations> locations = generateScanRangeLocations(nodes, 1, 1, false);
        // Confirm our assumption that the TScanRangeLocations we used in the test has the 1st CN assigned.
        Assert.assertEquals(1, locations.get(0).locations.get(0).backend_id);

        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        CacheSelectComputeNodeSelectionProperties props =
                new CacheSelectComputeNodeSelectionProperties(List.of("group1", "group2"), 2, 0);
        CacheSelectBackendSelector selector =
                new CacheSelectBackendSelector(scanNode, locations, assignment, callerWorkerProvider, props,
                        DEFAULT_WAREHOUSE_ID);
        new Expectations() {
            {
                warehouseManager.getAllComputeNodeIds(DEFAULT_WAREHOUSE_ID);
                result = nodeIds;
            }
        };

        UserException exception = assertThrows(UserException.class, selector::computeScanRangeAssignment);
        Assert.assertEquals("Compute node not found. Check if any compute node is down." +
                " nodeId: -1 compute node: [whatever alive: true, available: false, inBlacklist: false]" +
                " [whatever alive: true, available: false, inBlacklist: false]" +
                " [whatever alive: true, available: true, inBlacklist: false] ", exception.getMessage());

    }

    @Test
    public void testBackupReplicasKnownTabletIdAndInternalMapping(@Mocked SystemInfoService systemInfoService,
                                                                  @Mocked WorkerProvider callerWorkerProvider,
                                                                  @Mocked WorkerGroupManager workerGroupManager,
                                                                  @Mocked StarOSAgent starOsAgent) throws StarClientException {
        ScanNode scanNode = newOlapScanNode(1, 1);
        TabletComputeNodeMapper tabletComputeNodeMapper = new TabletComputeNodeMapper();
        Map<Long, ComputeNode> nodes = LongStream.range(0L, 4L).mapToObj(id -> {
            ComputeNode cn = new ComputeNode(id, "somehost", 100);
            cn.setAlive(true);
            cn.setResourceIsolationGroup("somegroup");
            tabletComputeNodeMapper.addComputeNode(cn.getId(), cn.getResourceIsolationGroup());
            return cn;
        }).collect(Collectors.toMap(ComputeNode::getId, Function.identity()));

        List<TScanRangeLocations> locations = generateScanRangeLocations(nodes, 1, 1, true);
        long givenTabletId = 0L;
        Assert.assertEquals(givenTabletId, locations.get(0).scan_range.internal_scan_range.tablet_id);
        CacheSelectComputeNodeSelectionProperties props =
                new CacheSelectComputeNodeSelectionProperties(List.of("somegroup"), 0, 3);
        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        CacheSelectBackendSelector selector =
                new CacheSelectBackendSelector(scanNode, locations, assignment, callerWorkerProvider, props,
                        DEFAULT_WAREHOUSE_ID);

        Long starOsPreferredCnId = -1L;
        ShardInfo mockedShardInfo = ShardInfo.newBuilder().build();
        List<Long> expectedSelections = tabletComputeNodeMapper.backupComputeNodesForTablet(givenTabletId, starOsPreferredCnId,
                props.numBackupReplicasDesired, "somegroup");
        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);
        new Expectations(globalStateMgr) {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getStarOSAgent();
                result = starOsAgent;
            }
        };
        new Expectations() {
            {
                workerGroupManager.getWorkerGroup("somegroup");
                result = 1L;

                starOsAgent.getShardInfo(givenTabletId, 1L);
                result = mockedShardInfo;

                starOsAgent.getAllNodeIdsByShard(mockedShardInfo, true);
                result = starOsPreferredCnId;

                systemInfoService.internalTabletMapper();
                result = tabletComputeNodeMapper;

                for (Long cnId : expectedSelections) {
                    callerWorkerProvider.selectWorkerUnchecked(cnId);
                    minTimes = 1;
                }

                callerWorkerProvider.setAllowGetAnyWorker(true);
                times = 1;
            }
        };

        try {
            selector.computeScanRangeAssignment();
        } catch (UserException e) {
            throw new RuntimeException(e);
        } finally {
            int givenScanNodeId = scanNode.getId().asInt();
            TScanRangeParams givenRangeParams = new TScanRangeParams(locations.get(0).scan_range);
            FragmentScanRangeAssignment expectedAssignment = new FragmentScanRangeAssignment();
            expectedSelections.forEach(id -> expectedAssignment.put(id, givenScanNodeId, givenRangeParams));
            Assert.assertEquals(expectedAssignment, assignment);
        }
    }

    // Important test: checks that CacheSelectBackendSelector places second replica in same place that the
    // DefaultSharedDataWorkerProvider would choose as a backup for the given tablet.
    @Test
    public void testChosenCnMatchDefaultBackup(@Mocked SystemInfoService systemInfoService,
                                               @Mocked WorkerProvider callerWorkerProvider,
                                               @Mocked WorkerGroupManager workerGroupManager, @Mocked StarOSAgent starOsAgent)
            throws StarClientException {
        ScanNode scanNode = newOlapScanNode(1, 1);
        TabletComputeNodeMapper tabletComputeNodeMapper = new TabletComputeNodeMapper();
        Map<Long, ComputeNode> nodes = LongStream.range(1L, 100L).mapToObj(id -> {
            ComputeNode cn = new ComputeNode(id, "somehost", 100);
            cn.setAlive(true);
            cn.setResourceIsolationGroup("somegroup");
            tabletComputeNodeMapper.addComputeNode(cn.getId(), cn.getResourceIsolationGroup());
            return cn;
        }).collect(Collectors.toMap(ComputeNode::getId, Function.identity()));

        List<TScanRangeLocations> locations = generateScanRangeLocations(nodes, 1, 1, true);
        long givenTabletId = 1L;
        locations.get(0).scan_range.internal_scan_range.tablet_id = givenTabletId;
        Assert.assertEquals(givenTabletId, locations.get(0).scan_range.internal_scan_range.tablet_id);
        CacheSelectComputeNodeSelectionProperties props =
                new CacheSelectComputeNodeSelectionProperties(List.of("somegroup"), 0, 1);
        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        CacheSelectBackendSelector selector =
                new CacheSelectBackendSelector(scanNode, locations, assignment, callerWorkerProvider, props,
                        DEFAULT_WAREHOUSE_ID);

        Long starOsPreferredCnId = tabletComputeNodeMapper.backupComputeNodesForTablet(givenTabletId, -1L, 1, "somegroup").get(0);
        ShardInfo mockedShardInfo = ShardInfo.newBuilder().build();
        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);
        new Expectations(globalStateMgr) {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getStarOSAgent();
                result = starOsAgent;
            }
        };
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        new Expectations(warehouseManager) {
            {
                warehouseManager.getAllComputeNodeIds(anyLong);
                result = Lists.newArrayList(nodes.keySet());
                minTimes = 0;
            }
        };
        NodeMgr nodeMgr = GlobalStateMgr.getCurrentState().getNodeMgr();
        Frontend thisFe = new Frontend();
        thisFe.setResourceIsolationGroup("somegroup");

        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                ComputeNode node = nodes.get(nodeId);
                return node;
            }
        };
        new Expectations(nodeMgr) {
            {
                nodeMgr.getMySelf();
                result = thisFe;
                minTimes = 0;

                nodeMgr.getClusterInfo();
                result = systemInfoService;
            }
        };
        new Expectations() {
            {
                workerGroupManager.getWorkerGroup("somegroup");
                result = 1L;

                starOsAgent.getShardInfo(givenTabletId, 1L);
                result = mockedShardInfo;

                starOsAgent.getAllNodeIdsByShard(mockedShardInfo, true);
                result = starOsPreferredCnId;

                systemInfoService.internalTabletMapper();
                result = tabletComputeNodeMapper;
            }
        };

        try {
            selector.computeScanRangeAssignment();
        } catch (UserException e) {
            throw new RuntimeException(e);
        } finally {
            Assert.assertEquals(1, assignment.size());
            long cacheSelectCnSelectedAsBackup = assignment.keySet().iterator().next();
            Assert.assertNotEquals(starOsPreferredCnId.longValue(), cacheSelectCnSelectedAsBackup);
            DefaultSharedDataWorkerProvider.Factory factory = new DefaultSharedDataWorkerProvider.Factory();
            DefaultSharedDataWorkerProvider provider =
                    factory.captureAvailableWorkers(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(), true, -1,
                            SessionVariableConstants.ComputationFragmentSchedulingPolicy.COMPUTE_NODES_ONLY,
                            WarehouseManager.DEFAULT_WAREHOUSE_ID);

            Assert.assertEquals(provider.selectBackupWorker(starOsPreferredCnId, Optional.of(givenTabletId)),
                    cacheSelectCnSelectedAsBackup);
        }
    }

    @Test
    public void testBackupReplicasUnknownTabletId(@Mocked WarehouseManager warehouseManager,
                                                  @Mocked SystemInfoService systemInfoService,
                                                  @Mocked WorkerProvider callerWorkerProvider) {
        int backupReplicas = 2;
        ScanNode scanNode = newOlapScanNode(1, 1);
        List<Long> somegroup1Ids = List.of(0L, 3L, 4L, 8L);
        List<Long> somegroup2Ids = List.of(1L, 2L, 7L);
        List<Long> expectedIds = Stream.concat(somegroup1Ids.stream().skip(1).limit(backupReplicas),
                somegroup2Ids.stream().skip(1).limit(backupReplicas)).collect(Collectors.toList());
        Map<Long, ComputeNode> nodes = LongStream.range(0L, 10L).mapToObj(id -> {
            ComputeNode cn = new ComputeNode(id, "somehost", 100);
            cn.setAlive(true);
            if (somegroup1Ids.contains(id)) {
                cn.setResourceIsolationGroup("somegroup1");
            } else if (somegroup2Ids.contains(id)) {
                cn.setResourceIsolationGroup("somegroup2");
            } else {
                cn.setResourceIsolationGroup("somegroup3");
            }
            return cn;
        }).collect(Collectors.toMap(ComputeNode::getId, Function.identity()));
        List<Long> nodeIds = new ArrayList<>(nodes.keySet());

        nodes.values().forEach(cn -> {
            new Expectations() {
                {
                    systemInfoService.getBackendOrComputeNode(cn.getId());
                    result = cn;
                }
            };
        });

        // Non-internal scans don't have tabletIds and should therefore use the workerProviders to get backups.
        List<TScanRangeLocations> locations = generateScanRangeLocations(nodes, 1, 1, false);
        // Confirm our assumption that the TScanRangeLocations we used in the test has the 0th CN assigned.
        Assert.assertEquals(0, locations.get(0).locations.get(0).backend_id);

        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        CacheSelectComputeNodeSelectionProperties props =
                new CacheSelectComputeNodeSelectionProperties(List.of("somegroup1", "somegroup2"), 0, backupReplicas);
        CacheSelectBackendSelector selector =
                new CacheSelectBackendSelector(scanNode, locations, assignment, callerWorkerProvider, props,
                        DEFAULT_WAREHOUSE_ID);
        new Expectations() {
            {
                warehouseManager.getAllComputeNodeIds(DEFAULT_WAREHOUSE_ID);
                result = nodeIds;

                expectedIds.forEach(id -> {
                    callerWorkerProvider.selectWorkerUnchecked(id);
                    minTimes = 1;
                });

                callerWorkerProvider.setAllowGetAnyWorker(true);
                times = 1;
            }
        };

        try {
            selector.computeScanRangeAssignment();
        } catch (UserException e) {
            throw new RuntimeException(e);
        } finally {
            int givenScanNodeId = scanNode.getId().asInt();
            TScanRangeParams givenRangeParams = new TScanRangeParams(locations.get(0).scan_range);
            FragmentScanRangeAssignment expectedAssignment = new FragmentScanRangeAssignment();
            expectedIds.forEach(id -> expectedAssignment.put(id, givenScanNodeId, givenRangeParams));
            Assert.assertEquals(expectedAssignment, assignment);
        }

    }

    @Test
    public void testNoResourceIsolationGroupException(@Mocked WarehouseManager warehouseManager,
                                                      @Mocked SystemInfoService systemInfoService,
                                                      @Mocked WorkerProvider callerWorkerProvider) {
        ScanNode scanNode = newOlapScanNode(1, 1);
        Map<Long, ComputeNode> nodes = LongStream.range(1L, 3L).mapToObj(id -> {
            ComputeNode cn = new ComputeNode(id, "somehost", 100);
            cn.setAlive(true);
            cn.setResourceIsolationGroup("knowngroup");
            return cn;
        }).collect(Collectors.toMap(ComputeNode::getId, Function.identity()));
        List<Long> nodeIds = new ArrayList<>(nodes.keySet());

        nodes.values().forEach(cn -> {
            new Expectations() {
                {
                    systemInfoService.getBackendOrComputeNode(cn.getId());
                    result = cn;
                }
            };
        });

        // Non-internal scans don't have tabletIds and should therefore use the workerProviders to get backups.
        List<TScanRangeLocations> locations = generateScanRangeLocations(nodes, 1, 1, false);
        // Confirm our assumption that the TScanRangeLocations we used in the test has the 1st CN assigned.
        Assert.assertEquals(1, locations.get(0).locations.get(0).backend_id);

        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        CacheSelectComputeNodeSelectionProperties props =
                new CacheSelectComputeNodeSelectionProperties(List.of("unknowngroup"), 1, 0);
        CacheSelectBackendSelector selector =
                new CacheSelectBackendSelector(scanNode, locations, assignment, callerWorkerProvider, props,
                        DEFAULT_WAREHOUSE_ID);
        new Expectations() {
            {
                warehouseManager.getAllComputeNodeIds(DEFAULT_WAREHOUSE_ID);
                result = nodeIds;
            }
        };

        UserException exception = assertThrows(UserException.class, selector::computeScanRangeAssignment);
        Assert.assertEquals("No CN nodes available for the specified resource group. resourceGroup: unknowngroup",
                exception.getMessage());

    }

    @Test
    public void testNoResourceIsolationGroupExceptionInternalMapping(@Mocked SystemInfoService systemInfoService,
                                                                     @Mocked WorkerProvider callerWorkerProvider,
                                                                     @Mocked WorkerGroupManager workerGroupManager,
                                                                     @Mocked StarOSAgent starOsAgent) throws StarClientException {
        ScanNode scanNode = newOlapScanNode(1, 1);
        TabletComputeNodeMapper tabletComputeNodeMapper = new TabletComputeNodeMapper();
        Map<Long, ComputeNode> nodes = LongStream.range(0L, 3L).mapToObj(id -> {
            ComputeNode cn = new ComputeNode(id, "somehost", 100);
            cn.setAlive(true);
            cn.setResourceIsolationGroup("knowngroup");
            tabletComputeNodeMapper.addComputeNode(cn.getId(), cn.getResourceIsolationGroup());
            return cn;
        }).collect(Collectors.toMap(ComputeNode::getId, Function.identity()));

        List<TScanRangeLocations> locations = generateScanRangeLocations(nodes, 1, 1, true);
        long givenTabletId = 0L;
        Assert.assertEquals(givenTabletId, locations.get(0).scan_range.internal_scan_range.tablet_id);
        CacheSelectComputeNodeSelectionProperties props =
                new CacheSelectComputeNodeSelectionProperties(List.of("unknowngroup"), 0, 1);
        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        CacheSelectBackendSelector selector =
                new CacheSelectBackendSelector(scanNode, locations, assignment, callerWorkerProvider, props,
                        DEFAULT_WAREHOUSE_ID);

        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);
        new Expectations(globalStateMgr) {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getStarOSAgent();
                result = starOsAgent;
            }
        };
        Long starOsPreferredCnId = -1L;
        ShardInfo mockedShardInfo = ShardInfo.newBuilder().build();
        new Expectations() {
            {
                workerGroupManager.getWorkerGroup("unknowngroup");
                result = 1L;

                starOsAgent.getShardInfo(givenTabletId, 1L);
                result = mockedShardInfo;

                starOsAgent.getAllNodeIdsByShard(mockedShardInfo, true);
                result = starOsPreferredCnId;

                systemInfoService.internalTabletMapper();
                result = tabletComputeNodeMapper;
            }
        };

        UserException exception = assertThrows(UserException.class, selector::computeScanRangeAssignment);
        Assert.assertEquals(
                "No CN nodes available for the specified resource group." + " resourceGroup: unknowngroup, tabletId: 0",
                exception.getMessage());

    }
}
