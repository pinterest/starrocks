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

import com.google.common.collect.ImmutableList;
import com.starrocks.common.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.SystemInfoService;
import com.starrocks.system.TabletComputeNodeMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/*
    This class is responsible for giving information about Tablets. It is limited in that it will only report information about
     tablets which this FE has needed to scan to fulfill a query for multiple replicas which it received since it came up.
     This is meant to help us understand if some tablet is very hot right now, and to know which CN is/are handling that load.
 */
public class TabletMappingProcNode implements ProcDirInterface {

    private static final Logger LOG = LogManager.getLogger(TabletMappingProcNode.class);

    public static final ImmutableList<String> TITLE_NAMES;

    static {
        ImmutableList.Builder<String> builder =
                new ImmutableList.Builder<String>().add("TabletId").add("BackupRequestCount").add("PrimaryCnOwner")
                        .add("SecondaryCnOwner");
        TITLE_NAMES = builder.build();
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        final SystemInfoService clusterInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        if (clusterInfoService.internalTabletMapper() == null) {
            LOG.warn("Requesting tablet mapping information but it's not internally maintained.");
            BaseProcResult result = new BaseProcResult();
            result.setNames(List.of("N/A"));
            result.addRow(List.of("Tablet mapping information not internally maintained"));
            return result;
        }

        final TabletComputeNodeMapper tabletComputeNodeMapper = clusterInfoService.internalTabletMapper();
        // TODO(cbrennan) Make this work even when we haven't asked for backup replicas through cache select.
        Map<Long, AtomicLong> tabletToMappingCount = tabletComputeNodeMapper.getTabletMappingCount();
        if (tabletToMappingCount.isEmpty()) {
            LOG.warn("Cannot provide tablet mapping information when we haven't requested backup/multiple cache replicas.");
            BaseProcResult result = new BaseProcResult();
            result.setNames(List.of("N/A"));
            result.addRow(List.of("Backup tablet mapping information not yet requested"));
            return result;
        }
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        String thisRig = GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf().getResourceIsolationGroup();
        for (Map.Entry<Long, AtomicLong> entry : tabletToMappingCount.entrySet()) {
            List<String> tabletInfo = new ArrayList<>(TITLE_NAMES.size());
            Long tabletId = entry.getKey();
            long backupRequestCount = entry.getValue().get();

            tabletInfo.add(tabletId.toString());
            tabletInfo.add(Long.toString(backupRequestCount));

            Long primaryCnOwner = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                    .getComputeNodeId(WarehouseManager.DEFAULT_WAREHOUSE_ID, tabletId);

            tabletInfo.add(primaryCnOwner.toString());

            // We ask for 1 compute node ids since we're outputting the main tablet owner and the first backup.
            List<Long> backupCn = tabletComputeNodeMapper.backupComputeNodesForTablet(tabletId, primaryCnOwner, 1, thisRig);
            if (backupCn.isEmpty()) {
                // If there's only 1 CN in this resource isolation group, there can be no SecondaryCnOwner,
                // fill in the row to reflect that.
                tabletInfo.add("N/A");
            } else {
                tabletInfo.add(backupCn.get(0).toString());
            }
            result.addRow(tabletInfo);
        }
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return true;
    }

    @Override
    public ProcNodeInterface lookup(String name) throws AnalysisException {
        return null;
    }

}
