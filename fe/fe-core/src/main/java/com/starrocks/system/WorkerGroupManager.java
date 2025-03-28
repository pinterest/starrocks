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
package com.starrocks.system;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WorkerGroupManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(WorkerGroupManager.class);

    // writeLock must be acquired when we might create a new worker group.
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private final Lock writeLock = stateLock.writeLock();

    @SerializedName("rigToWgid")
    private final Map<String, Long> resourceIsolationGroupToWorkerGroupId = new HashMap<>();

    public WorkerGroupManager() {
    }

    public Optional<Long> getWorkerGroup(String resourceIsolationGroup) {
        if (ResourceIsolationGroupUtils.resourceIsolationGroupMatches(resourceIsolationGroup,
                ResourceIsolationGroupUtils.DEFAULT_RESOURCE_ISOLATION_GROUP_ID)) {
            return Optional.of(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
        }
        if (resourceIsolationGroupToWorkerGroupId.containsKey(resourceIsolationGroup)) {
            return Optional.of(resourceIsolationGroupToWorkerGroupId.get(resourceIsolationGroup));
        }
        writeLock.lock();
        try {
            // Check again in case some other thread updated the map between last time we checked and when we managed to
            // acquire writeLock.
            if (resourceIsolationGroupToWorkerGroupId.containsKey(resourceIsolationGroup)) {
                return Optional.of(resourceIsolationGroupToWorkerGroupId.get(resourceIsolationGroup));
            }
            LOG.info("Going to StarOS to get worker group mapping for resource isolation group: {}", resourceIsolationGroup);
            Optional<Long> workerGroupId =
                    GlobalStateMgr.getCurrentState().getStarOSAgent().tryGetWorkerGroupForOwner(resourceIsolationGroup);
            if (workerGroupId.isEmpty()) {
                LOG.warn("No worker group id found for resource isolation group {}", resourceIsolationGroup);
                return Optional.empty();
            }
            LOG.info("Stored new mapping from resource isolation group {} to worker group {}", resourceIsolationGroup,
                    workerGroupId.get());
            resourceIsolationGroupToWorkerGroupId.put(resourceIsolationGroup, workerGroupId.get());
            return workerGroupId;
        } finally {
            writeLock.unlock();
        }

    }

    public Long getOrCreateWorkerGroup(String resourceIsolationGroup) {
        if (ResourceIsolationGroupUtils.resourceIsolationGroupMatches(resourceIsolationGroup,
                ResourceIsolationGroupUtils.DEFAULT_RESOURCE_ISOLATION_GROUP_ID)) {
            return StarOSAgent.DEFAULT_WORKER_GROUP_ID;
        }
        if (resourceIsolationGroupToWorkerGroupId.containsKey(resourceIsolationGroup)) {
            return resourceIsolationGroupToWorkerGroupId.get(resourceIsolationGroup);
        }
        writeLock.lock();
        try {
            // Check again in case some other thread updated the map between last time we checked and when we managed to
            // acquire writeLock.
            if (resourceIsolationGroupToWorkerGroupId.containsKey(resourceIsolationGroup)) {
                return resourceIsolationGroupToWorkerGroupId.get(resourceIsolationGroup);
            }
            LOG.info("Going to StarOS to get or create worker group mapping for resource isolation group: {}",
                    resourceIsolationGroup);
            long workerGroupId =
                    GlobalStateMgr.getCurrentState().getStarOSAgent().getOrCreateWorkerGroupForOwner(resourceIsolationGroup);
            LOG.info("Stored new mapping from resource isolation group {} to worker group {}", resourceIsolationGroup,
                    workerGroupId);
            resourceIsolationGroupToWorkerGroupId.put(resourceIsolationGroup, workerGroupId);
            return workerGroupId;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}
