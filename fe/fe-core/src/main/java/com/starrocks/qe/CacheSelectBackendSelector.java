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

import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.lake.qe.scheduler.DefaultSharedDataWorkerProvider;
import com.starrocks.planner.ScanNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.system.TabletComputeNodeMapper;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.starrocks.qe.scheduler.Utils.getOptionalTabletId;

// This class should only be used in shared data mode.
public class CacheSelectBackendSelector implements BackendSelector {
    private static final Logger LOG = LogManager.getLogger(NormalBackendSelector.class);

    // Inputs
    private final ScanNode scanNode;
    private final List<TScanRangeLocations> locations;
    private final CacheSelectComputeNodeSelectionProperties props;
    private final long warehouseId;

    // Outputs
    private final FragmentScanRangeAssignment assignment;
    private final Set<Long> allSelectedWorkerIds;

    public CacheSelectBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                                      FragmentScanRangeAssignment assignment,
                                      CacheSelectComputeNodeSelectionProperties props, long warehouseId) {
        this.scanNode = scanNode;
        this.locations = locations;
        this.assignment = assignment;
        this.props = props;
        this.warehouseId = warehouseId;
        this.allSelectedWorkerIds = new HashSet<>();
    }

    public Set<Long> getSelectedWorkerIds() {
        return allSelectedWorkerIds;
    }

    private Set<Long> getAssignedCnByTabletId(SystemInfoService systemInfoService, Long tabletId,
                                              String resourceIsolationGroupId) throws UserException {
        TabletComputeNodeMapper mapper = systemInfoService.internalTabletMapper();
        List<Long> cnIdsOrderedByPreference = mapper.computeNodesForTablet(
                tabletId, props.numReplicasDesired, resourceIsolationGroupId);
        if (cnIdsOrderedByPreference.size() < props.numReplicasDesired) {
            throw new DdlException(String.format("Requesting more replicas than we have available CN" +
                            " for the specified resource group. desiredReplicas: %d, resourceGroup: %s",
                    props.numReplicasDesired, resourceIsolationGroupId));
        }
        return new HashSet<>(cnIdsOrderedByPreference);
    }

    private Set<Long> getAssignedCnByBackupForTargetCn(Long mainTargetCnId, String resourceIsolationGroupId)
            throws UserException {
        Set<Long> selectedCn = new HashSet<>();
        DefaultSharedDataWorkerProvider workerProvider = new DefaultSharedDataWorkerProvider.Factory().
                captureAvailableWorkers(warehouseId, resourceIsolationGroupId);
        long targetBackendId = mainTargetCnId;
        while (selectedCn.size() < props.numReplicasDesired) {
            if (selectedCn.contains(targetBackendId) ||
                    !workerProvider.isDataNodeAvailable(targetBackendId)) {
                targetBackendId = workerProvider.selectBackupWorker(targetBackendId, Optional.empty());
                if (selectedCn.contains(targetBackendId)) {
                    workerProvider.reportDataNodeNotFoundException();
                    throw new DdlException(String.format("Requesting more replicas than we have available CN" +
                                    " for the specified resource group. desiredReplicas: %d, resourceGroup: %s",
                            props.numReplicasDesired, resourceIsolationGroupId));
                }
            }
            selectedCn.add(targetBackendId);
        }
        return selectedCn;
    }

    @Override
    public void computeScanRangeAssignment() throws UserException {
        if (props.resourceIsolationGroups == null || props.resourceIsolationGroups.isEmpty()) {
            throw new UserException("Should not have constructed CacheSelectBackendSelector with no" +
                    " resourceIsolationGroups specified.");
        }
        if (props.numReplicasDesired < 1) {
            throw new UserException("Num replicas desired in cache must be at least 1: " + props.numReplicasDesired);
        }

        SystemInfoService systemInfoService =
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        // Try to create assignments for each of the resourceIsolationGroups specified.
        for (TScanRangeLocations scanRangeLocations : locations) {
            if (scanRangeLocations.getLocationsSize() != 1) {
                throw new UserException("CacheSelectBackendSelector expected to be used in situations where there" +
                        " is exactly one CN to which any given tablet is officially assigned: " +
                        scanRangeLocations);
            }
            TScanRangeParams scanRangeParams = new TScanRangeParams(scanRangeLocations.scan_range);
            Optional<Long> tabletId = getOptionalTabletId(scanRangeLocations.scan_range);
            for (String resourceIsolationGroupId : props.resourceIsolationGroups) {
                Set<Long> selectedCn;
                // If we've been provided the relevant tablet id, and we're using resource isolation groups, which
                // is when we prefer to use the internal mapping, then we populate the datacaches of the CN which
                // are most preferred for the tablet.
                if (tabletId.isPresent() && systemInfoService.shouldUseInternalTabletToCnMapper()) {
                    selectedCn = getAssignedCnByTabletId(systemInfoService, tabletId.get(),
                            resourceIsolationGroupId);
                } else {
                    selectedCn = getAssignedCnByBackupForTargetCn(
                            scanRangeLocations.getLocations().get(0).getBackend_id(), resourceIsolationGroupId);
                }
                for (Long cnId : selectedCn) {
                    assignment.put(cnId, scanNode.getId().asInt(), scanRangeParams);
                    allSelectedWorkerIds.add(cnId);
                }
            }
        }
    }
}