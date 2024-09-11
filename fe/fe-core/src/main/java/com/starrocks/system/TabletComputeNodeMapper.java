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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/system/SystemInfoService.java

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
import com.starrocks.catalog.Tablet;
import com.starrocks.common.util.ConsistentHashRing;
import com.starrocks.common.util.HashRing;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import org.jetbrains.annotations.TestOnly;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
 *
 */

public class TabletComputeNodeMapper {

    private static final int CONSISTENT_HASH_RING_VIRTUAL_NUMBER = 256;
    private static final Tablet ARBITRARY_FAKE_TABLET = new LakeTablet(1L);

    private class TabletMap {
        private final HashRing<Tablet, Long> tabletToComputeNodeId;
        private int numReplicas;

        TabletMap() {
            tabletToComputeNodeId = new ConsistentHashRing<>(
                    Hashing.murmur3_128(), new TabletFunnel(), new ComputeNodeIdFunnel(),
                    Collections.emptyList(), CONSISTENT_HASH_RING_VIRTUAL_NUMBER);
            numReplicas = 1;
        }

        private boolean tracksSomeComputeNode() {
            return !tabletToComputeNodeId.get(ARBITRARY_FAKE_TABLET, 1).isEmpty();
        }
    }

    private final Map<String, TabletMap> resourceIsolationGroupToTabletMapping;
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private final Lock readLock = stateLock.readLock();
    private final Lock writeLock = stateLock.writeLock();

    public TabletComputeNodeMapper() {
        resourceIsolationGroupToTabletMapping = new HashMap<>();
    }

    @TestOnly
    public void clear() {
        resourceIsolationGroupToTabletMapping.clear();
    }

    public int numResourceIsolationGroups() {
        return resourceIsolationGroupToTabletMapping.size();
    }

    public void setNumReplicas(String resourceIsolationGroup, int numReplicas) {
        writeLock.lock();
        try {
            maybeInitResourceIsolationGroup(resourceIsolationGroup);
            TabletMap map = resourceIsolationGroupToTabletMapping.get(resourceIsolationGroup);
            map.numReplicas = numReplicas;
        } finally {
            writeLock.unlock();
        }
    }

    public void addComputeNode(Long computeNodeId, String resourceIsolationGroup) {
        writeLock.lock();
        try {
            addComputeNodeUnsynchronized(computeNodeId, resourceIsolationGroup);
        } finally {
            writeLock.unlock();
        }
    }

    private void maybeInitResourceIsolationGroup(String resourceIsolationGroup) {
        if (resourceIsolationGroup == null) {
            resourceIsolationGroup = DEFAULT_RESOURCE_ISOLATION_GROUP_ID;
        }
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

    public void modifyComputeNode(Long computeNodeId,
                                  String oldResourceIsolationGroup, String newResourceIsolationGroup) {
        writeLock.lock();
        try {
            removeComputeNodeUnsynchronized(computeNodeId, oldResourceIsolationGroup);
            addComputeNodeUnsynchronized(computeNodeId, newResourceIsolationGroup);
        } finally {
            writeLock.unlock();
        }
    }

    public List<Long> computeNodesForTablet(Tablet tablet) {
        readLock.lock();
        try {
            String thisResourceIsolationGroup = GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf().
                    getResourceIsolationGroup();
            if (!this.resourceIsolationGroupToTabletMapping.containsKey(thisResourceIsolationGroup)) {
                return null;
            }
            TabletMap m = this.resourceIsolationGroupToTabletMapping.get(thisResourceIsolationGroup);
            return m.tabletToComputeNodeId.get(tablet, m.numReplicas);
        } finally {
            readLock.unlock();
        }
    }

    class ComputeNodeIdFunnel implements Funnel<Long> {
        @Override
        public void funnel(Long computeNodeId, PrimitiveSink primitiveSink) {
            primitiveSink.putLong(computeNodeId);
        }
    }

    class TabletFunnel implements Funnel<Tablet> {
        @Override
        public void funnel(Tablet tablet, PrimitiveSink primitiveSink) {
            primitiveSink.putLong(tablet.getId());
        }
    }
}
