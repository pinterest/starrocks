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
     tablets which this FE has needed to scan to fulfill a query which it received since it came up.
 */
public class TabletMappingProcNode implements ProcDirInterface {

    private static final Logger LOG = LogManager.getLogger(TabletMappingProcNode.class);

    public static final ImmutableList<String> TITLE_NAMES;

    static {
        ImmutableList.Builder<String> builder =
                new ImmutableList.Builder<String>().add("TabletId").add("RequestCount").add("PrimaryCnOwner")
                        .add("SecondaryCnOwner");
        TITLE_NAMES = builder.build();
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        final SystemInfoService clusterInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        if (!clusterInfoService.shouldUseInternalTabletToCnMapper()) {
            LOG.warn("Requesting tablet mapping information but it's not internally maintained.");
            BaseProcResult result = new BaseProcResult();
            result.setNames(List.of("N/A"));
            result.addRow(List.of("Tablet mapping information not internally maintained"));
            return result;
        }

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        final TabletComputeNodeMapper tabletComputeNodeMapper = clusterInfoService.internalTabletMapper();

        for (Map.Entry<Long, AtomicLong> tabletToMappingCount : tabletComputeNodeMapper.getTabletMappingCount().entrySet()) {
            List<String> tabletInfo = new ArrayList<>(TITLE_NAMES.size());

            tabletInfo.add(tabletToMappingCount.getKey().toString());
            tabletInfo.add(tabletToMappingCount.getValue().toString());

            List<Long> computeNodeIds = tabletComputeNodeMapper.computeNodesForTablet(tabletToMappingCount.getKey(), 2);
            for (Long computeNodeId : computeNodeIds) {
                tabletInfo.add(computeNodeId.toString());
            }
            while (tabletInfo.size() < TITLE_NAMES.size()) {
                // If there's only 1 CN in this resource isolation group, there can be no SecondaryCnOwner,
                // fill in the row to reflect that.
                tabletInfo.add("N/A");
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
