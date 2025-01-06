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
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.UserException;
import com.starrocks.lake.qe.scheduler.DefaultSharedDataWorkerProvider;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.system.TabletComputeNodeMapper;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.qe.scheduler.Utils.getOptionalTabletId;

// This class should only be used in shared data mode.
public class CacheSelectBackendSelector implements BackendSelector {
    private static final Logger LOG = LogManager.getLogger(CacheSelectBackendSelector.class);

    // Inputs
    private final ScanNode scanNode;
    private final List<TScanRangeLocations> locations;
    private final CacheSelectComputeNodeSelectionProperties props;
    private final long warehouseId;

    // Outputs
    private final FragmentScanRangeAssignment assignment;
    // This WorkerProvider is used to provide signal to the caller, but not used to select the compute nodes to use.
    private final WorkerProvider callerWorkerProvider;

    public CacheSelectBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                                      FragmentScanRangeAssignment assignment, WorkerProvider callerWorkerProvider,
                                      CacheSelectComputeNodeSelectionProperties props, long warehouseId) {
        this.scanNode = scanNode;
        this.locations = locations;
        this.assignment = assignment;
        this.callerWorkerProvider = callerWorkerProvider;
        this.props = props;
        this.warehouseId = warehouseId;
    }

    private Set<Long> assignedCnByTabletId(SystemInfoService systemInfoService, Long tabletId,
                                           String resourceIsolationGroupId) throws UserException {
        TabletComputeNodeMapper mapper = systemInfoService.internalTabletMapper();
        int count = Math.max(props.numReplicasDesired, props.numBackupReplicasDesired);
        // skipCount variable uses for backup cache replicas CN nodes selection
        // to skip first CN node (primary) from the selected nodes
        int skipCount = props.numBackupReplicasDesired > 0 ? 1 : 0;
        List<Long> cnIdsOrderedByPreference =
                mapper.computeNodesForTablet(tabletId, count, resourceIsolationGroupId, skipCount);
        if (cnIdsOrderedByPreference.isEmpty()) {
            throw new DdlException(
                    String.format("No CN nodes available for the specified resource group." +
                                    " resourceGroup: %s, tabletId: %d",
                            resourceIsolationGroupId, tabletId));
        }
        if (cnIdsOrderedByPreference.size() < count) {
            throw new DdlException(
                    String.format("Requesting more replicas than we have available CN" +
                                    " for the specified resource group. desiredReplicas: %d," +
                                    " desiredBackupReplicas: %d, resourceGroup: %s, tabletId: %d",
                            props.numReplicasDesired, props.numBackupReplicasDesired,
                            resourceIsolationGroupId, tabletId));
        }
        return new HashSet<>(cnIdsOrderedByPreference);
    }

    private Set<Long> assignedCnByBackupWorker(Long mainTargetCnId, String resourceIsolationGroupId)
            throws UserException {
        DefaultSharedDataWorkerProvider workerProvider;
        try {
            workerProvider = new DefaultSharedDataWorkerProvider.Factory().captureAvailableWorkers(warehouseId,
                    resourceIsolationGroupId);
        } catch (ErrorReportException ex) {
            // captureAvailableWorkers() can throw an ErrorReportException (RuntimeException) with
            // error code as ERR_NO_NODES_IN_WAREHOUSE, which should be considered as
            // expected behaviour in this particular case, so transforming it to checked DdlException
            // would be consistent with this class logic.
            if (ex.getErrorCode() == ErrorCode.ERR_NO_NODES_IN_WAREHOUSE) {
                throw new DdlException(
                        String.format("No CN nodes available for the specified resource group. resourceGroup: %s",
                                resourceIsolationGroupId));
            } else {
                throw ex;
            }
        }
        List<Long> selectedCn = new ArrayList<>();
        int count = Math.max(props.numReplicasDesired, props.numBackupReplicasDesired);
        // skipCount variable uses for backup cache replicas CN nodes selection
        // to skip first CN node (primary) from the selected nodes
        int skipCount = props.numBackupReplicasDesired > 0 ? 1 : 0;
        long targetBackendId = mainTargetCnId;
        while (selectedCn.size() < count + skipCount) {
            if (selectedCn.contains(targetBackendId) || !workerProvider.isDataNodeAvailable(targetBackendId)) {
                targetBackendId = workerProvider.selectBackupWorker(targetBackendId, Optional.empty());
                if (targetBackendId < 0 || selectedCn.contains(targetBackendId)) {
                    workerProvider.reportDataNodeNotFoundException();
                    throw new DdlException(String.format("Requesting more replicas than we have available CN" +
                                    " for the specified resource group. desiredReplicas: %d, resourceGroup: %s",
                            props.numReplicasDesired, resourceIsolationGroupId));
                }
            }
            selectedCn.add(targetBackendId);
        }
        if (selectedCn.isEmpty()) {
            throw new DdlException(
                    String.format("No CN nodes available for the specified resource group. resourceGroup: %s",
                            resourceIsolationGroupId));
        }
        return selectedCn.stream().skip(skipCount).collect(Collectors.toSet());
    }

    @Override
    public void computeScanRangeAssignment() throws UserException {
        if (props.resourceIsolationGroups.isEmpty()) {
            throw new UserException("Should not have constructed CacheSelectBackendSelector with no" +
                    " resourceIsolationGroups specified.");
        }
        if (props.numReplicasDesired < 1 && props.numBackupReplicasDesired < 1) {
            throw new UserException(String.format(
                    "Num replicas or backup replicas desired in cache must be at least 1: replicas [%d] backup replicas [%d]",
                    props.numReplicasDesired, props.numBackupReplicasDesired));
        }

        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        Set<Long> allSelectedWorkerIds = new HashSet<>();
        for (TScanRangeLocations scanRangeLocations : locations) {
            TScanRangeParams scanRangeParams = new TScanRangeParams(scanRangeLocations.scan_range);
            Optional<Long> tabletId = getOptionalTabletId(scanRangeLocations.scan_range);
            // Try to create assignments for each of the resourceIsolationGroups specified.
            for (String resourceIsolationGroupId : props.resourceIsolationGroups) {
                Set<Long> selectedCn;
                // If we've been provided the relevant tablet id, and we're using resource isolation groups, which
                // is when we prefer to use the internal mapping, then we populate the datacaches of the CN which
                // are most preferred for the tablet.
                if (tabletId.isPresent() && systemInfoService.shouldUseInternalTabletToCnMapper()) {
                    selectedCn = assignedCnByTabletId(systemInfoService, tabletId.get(), resourceIsolationGroupId);
                } else {
                    if (scanRangeLocations.getLocationsSize() != 1) {
                        throw new UserException(
                                "CacheSelectBackendSelector expected to be used in situations where there is exactly" +
                                        " one CN to which any given tablet is officially assigned: " +
                                        scanRangeLocations);
                    }
                    selectedCn =
                            assignedCnByBackupWorker(scanRangeLocations.getLocations().get(0).getBackend_id(),
                                    resourceIsolationGroupId);
                }
                LOG.debug(String.format(
                        "done doing assignment for resource isolation group %s, tablet %d, location %s: CN chosen are %s",
                        resourceIsolationGroupId,
                        tabletId.orElse(-1L),
                        scanRangeLocations.getLocations().get(0),
                        selectedCn.stream().map(String::valueOf).collect(Collectors.joining(","))));

                for (Long cnId : selectedCn) {
                    assignment.put(cnId, scanNode.getId().asInt(), scanRangeParams);
                    allSelectedWorkerIds.add(cnId);
                }
            }
        }
        // Note that although we're not using the provided callerWorkerProvider above, the caller assumes that we used
        // it to note the selected backend ids. This is used for things like checking if the worker has died
        // and cancelling queries.
        allSelectedWorkerIds.forEach(callerWorkerProvider::selectWorkerUnchecked);

        // Also, caller upstream will use the workerProvider to get ComputeNode references corresponding to the compute
        // nodes chosen in this function, so we must enable getting any worker regardless of availability.
        callerWorkerProvider.setAllowGetAnyWorker(true);
    }
}