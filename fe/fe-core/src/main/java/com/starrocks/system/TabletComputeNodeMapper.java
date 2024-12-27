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

package com.starrocks.system;

import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import com.starrocks.common.util.ConsistentHashRing;
import com.starrocks.common.util.HashRing;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.TestOnly;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.starrocks.system.ResourceIsolationGroupUtils.DEFAULT_RESOURCE_ISOLATION_GROUP_ID;

/**
 * What:
 * Maps tablet to compute node(s) which are responsible for operations there-on.
 * Why:
 * Flexible replacement for StarOS/StarMgr management of the same type of mapping.
 * Useful for when the system is using resource-isolation groups, in that case each
 * ComputeNode should belong to exactly one TabletComputeNodeMapper (not necessarily on the same Frontend).
 * Notes on use:
 * Updated whenever there's an addition or removal of a compute node to the resource-isolation-group.
 * If using multiple replicas, consider the earlier-index CN for a given tablet to be more preferred
 * If some CN is removed, and it was the primary CN for some tablet, the first backup CN will become the primary
 * Thread-safe after initialization.
 */

public class TabletComputeNodeMapper {
    private static final Logger LOG = LogManager.getLogger(TabletComputeNodeMapper.class);

    private static final int CONSISTENT_HASH_RING_VIRTUAL_NUMBER = 256;
    private static final Long ARBITRARY_FAKE_TABLET = 1L;
    // We prefer to keep one replica of the data cached, unless otherwise requested with a CACHE SELECT STATEMENT.
    private static final int DEFAULT_NUM_REPLICAS = 1;

    private class TabletMap {
        private final HashRing<Long, Long> tabletToComputeNodeId;

        TabletMap() {
            tabletToComputeNodeId =
                    new ConsistentHashRing<>(Hashing.murmur3_128(), new LongIdFunnel(), new LongIdFunnel(),
                            Collections.emptyList(), CONSISTENT_HASH_RING_VIRTUAL_NUMBER);
        }

        // Returns whether any single compute node has been added to this TabletMap
        private boolean tracksSomeComputeNode() {
            return !tabletToComputeNodeId.get(ARBITRARY_FAKE_TABLET, 1).isEmpty();
        }
    }

    private final Map<String, TabletMap> resourceIsolationGroupToTabletMapping;
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private final Lock readLock = stateLock.readLock();
    private final Lock writeLock = stateLock.writeLock();

    // Tracking usage since the instantiation of this Mapper.
    private final ConcurrentMap<Long, AtomicLong> tabletMappingCount = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, AtomicLong> computeNodeReturnCount = new ConcurrentHashMap<>();

    public TabletComputeNodeMapper() {
        resourceIsolationGroupToTabletMapping = new HashMap<>();
    }

    @TestOnly
    public void clear() {
        resourceIsolationGroupToTabletMapping.clear();
        tabletMappingCount.clear();
        computeNodeReturnCount.clear();
    }

    public int numResourceIsolationGroups() {
        return resourceIsolationGroupToTabletMapping.size();
    }

    public boolean trackingNonDefaultResourceIsolationGroup() {
        int numGroups = numResourceIsolationGroups();
        if (numGroups == 0) {
            return false;
        }
        return numGroups > 1 || !resourceIsolationGroupToTabletMapping.containsKey(DEFAULT_RESOURCE_ISOLATION_GROUP_ID);
    }

    private String getResourceIsolationGroupName(String resourceIsolationGroup) {
        return resourceIsolationGroup == null ? DEFAULT_RESOURCE_ISOLATION_GROUP_ID : resourceIsolationGroup;
    }

    public String debugString() {
        return resourceIsolationGroupToTabletMapping.entrySet().stream()
                .map(entry -> String.format("%-15s : %s", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining("\n"));
    }

    public void addComputeNode(Long computeNodeId, String resourceIsolationGroup) {
        resourceIsolationGroup = getResourceIsolationGroupName(resourceIsolationGroup);
        LOG.info("Adding the cn {} to resource isolation group {}", computeNodeId, resourceIsolationGroup);
        writeLock.lock();
        try {
            addComputeNodeUnsynchronized(computeNodeId, resourceIsolationGroup);
        } finally {
            writeLock.unlock();
        }
    }

    private void maybeInitResourceIsolationGroup(String resourceIsolationGroup) {
        if (!resourceIsolationGroupToTabletMapping.containsKey(resourceIsolationGroup)) {
            resourceIsolationGroupToTabletMapping.put(resourceIsolationGroup, new TabletMap());
        }
    }

    private void addComputeNodeUnsynchronized(long computeNodeId, String resourceIsolationGroup) {
        maybeInitResourceIsolationGroup(resourceIsolationGroup);
        TabletMap map = resourceIsolationGroupToTabletMapping.get(resourceIsolationGroup);
        map.tabletToComputeNodeId.addNode(computeNodeId);
    }

    // This will succeed even if the resource isolation group is not being tracked.
    public void removeComputeNode(Long computeNodeId, String resourceIsolationGroup) {
        resourceIsolationGroup = getResourceIsolationGroupName(resourceIsolationGroup);
        LOG.info("Removing the cn {} from resource isolation group {}", computeNodeId, resourceIsolationGroup);
        writeLock.lock();
        try {
            removeComputeNodeUnsynchronized(computeNodeId, resourceIsolationGroup);
        } finally {
            writeLock.unlock();
        }
    }

    private void removeComputeNodeUnsynchronized(Long computeNodeId, String resourceIsolationGroup) {
        TabletMap map = resourceIsolationGroupToTabletMapping.get(resourceIsolationGroup);
        if (map == null) {
            return;
        }
        map.tabletToComputeNodeId.removeNode(computeNodeId);
        if (!map.tracksSomeComputeNode()) {
            resourceIsolationGroupToTabletMapping.remove(resourceIsolationGroup);
        }
    }

    public void modifyComputeNode(Long computeNodeId, String oldResourceIsolationGroup,
                                  String newResourceIsolationGroup) {
        oldResourceIsolationGroup = getResourceIsolationGroupName(oldResourceIsolationGroup);
        newResourceIsolationGroup = getResourceIsolationGroupName(newResourceIsolationGroup);
        LOG.info("Modifying the resource isolation group for cn {} from {} to {}", computeNodeId,
                oldResourceIsolationGroup,
                newResourceIsolationGroup);
        // We run the following even if oldResourceIsolationGroup.equals(newResourceIsolationGroup)
        // because we want to cleanly handle edge cases where the compute node hasn't already been
        // added to the TabletComputeNode mapper. This can happen in at least one situation, which
        // is when the cluster is first upgraded to include resource isolation groups.
        // Because the host ips match during a CN restart, upstream code which adds the ComputeNodes
        // will not execute and therefore we won't call this.addComputeNode.
        writeLock.lock();
        try {
            removeComputeNodeUnsynchronized(computeNodeId, oldResourceIsolationGroup);
            addComputeNodeUnsynchronized(computeNodeId, newResourceIsolationGroup);
        } finally {
            writeLock.unlock();
        }
    }

    public List<Long> computeNodesForTablet(Long tabletId) {
        return computeNodesForTablet(tabletId, DEFAULT_NUM_REPLICAS);
    }

    public List<Long> computeNodesForTablet(Long tabletId, int count) {
        String thisResourceIsolationGroup =
                GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf().getResourceIsolationGroup();
        return computeNodesForTablet(tabletId, count, thisResourceIsolationGroup, 0);
    }

    public List<Long> computeNodesForTablet(Long tabletId, int count, String resourceIsolationGroup, int skipCount) {
        readLock.lock();
        try {
            if (!this.resourceIsolationGroupToTabletMapping.containsKey(resourceIsolationGroup)) {
                LOG.warn(String.format(
                        "Requesting node for resource isolation group %s, to which" +
                                " there is not a known CN assigned.",
                        resourceIsolationGroup));
                return Collections.emptyList();
            }
            TabletMap m = this.resourceIsolationGroupToTabletMapping.get(resourceIsolationGroup);
            List<Long> computeNodes = m.tabletToComputeNodeId.get(tabletId, count + skipCount).stream().skip(skipCount)
                    .collect(Collectors.toList());
            // Update tracking information
            tabletMappingCount.computeIfAbsent(tabletId, k -> new AtomicLong()).incrementAndGet();
            computeNodes.forEach(
                    nodeId -> computeNodeReturnCount.computeIfAbsent(nodeId, k -> new AtomicLong()).incrementAndGet());
            return computeNodes;
        } finally {
            readLock.unlock();
        }
    }

    // Number of times that this mapper has returned the given compute node as the return value for `computeNodesForTablet`.
    public Long getComputeNodeReturnCount(Long computeNodeId) {
        return computeNodeReturnCount.getOrDefault(computeNodeId, new AtomicLong(0)).get();
    }

    // Returns the tablet mapped to the number of times which some caller has requested the appropriate CN.
    // Only reports the number of times since this particular FE came up.
    public ConcurrentMap<Long, AtomicLong> getTabletMappingCount() {
        return tabletMappingCount;
    }

    // Key is compute node id. Value is number of distinct tablets for which this mapper instance has returned the given CN.
    // Note, this is a kind of best-effort count -- it is possible that the given CN "owns" many more tablets, but we simply
    // haven't scanned them due to a query that has been issued to this FE since this mapper has been instantiated.
    // Note: this function is kind of expensive (~1 ms for largish databases), don't call it often.
    public Map<Long, Long> computeNodeToOwnedTabletCount() throws IllegalStateException {
        long startTime = System.currentTimeMillis();
        HashMap<Long, Long> computeNodeToOwnedTabletCount = new HashMap<>();
        for (Long tabletId : tabletMappingCount.keySet()) {
            List<Long> primaryCnForTablet = computeNodesForTablet(tabletId, 1);
            if (primaryCnForTablet.isEmpty()) {
                throw new IllegalStateException("No owner for tablet, seemingly no cn for resource isolation group");
            }
            // Instantiate to 1 or increment.
            computeNodeToOwnedTabletCount.merge(primaryCnForTablet.get(0), 1L, Long::sum);
        }
        LOG.info("millis passed calculating computeNodeToOwnedTabletCount: {}", System.currentTimeMillis() - startTime);
        return computeNodeToOwnedTabletCount;
    }

    class LongIdFunnel implements Funnel<Long> {
        @Override
        public void funnel(Long id, PrimitiveSink primitiveSink) {
            primitiveSink.putLong(id);
        }
    }

}
