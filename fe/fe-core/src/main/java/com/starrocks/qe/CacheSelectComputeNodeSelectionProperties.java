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

import com.starrocks.server.GlobalStateMgr;

import java.util.ArrayList;
import java.util.List;

// Describes how a CACHE SELECT statement should choose compute nodes to populate with the data.
// Defaults:
// if resource isolation groups are not specified in the CACHE SELECT statement, we assume the request intends to
// populate the data cache for the current FE's resource isolation group.
// If number of replicas is not specified in the CACHE SELECT statement, we assume the request intends to cache 1 replica.
public class CacheSelectComputeNodeSelectionProperties {
    public List<String> resourceIsolationGroups;
    public int numReplicasDesired;

    public CacheSelectComputeNodeSelectionProperties(List<String> resourceIsolationGroups, int numReplicasDesired) {
        if (resourceIsolationGroups == null || resourceIsolationGroups.isEmpty()) {
            this.resourceIsolationGroups = new ArrayList<>();
            this.resourceIsolationGroups.add(GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf()
                    .getResourceIsolationGroup());
        } else {
            this.resourceIsolationGroups = resourceIsolationGroups;
        }
        this.numReplicasDesired = Math.max(numReplicasDesired, 1);
    }
}
