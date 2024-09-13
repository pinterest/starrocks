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
package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.Frontend;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ShowResourceIsolationGroupStatement extends ShowStmt {

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("ResourceIsolationGroupId").add("FeCount").add("FeIds").add("CnCount").add("CnIds")
            .build();
    public ShowResourceIsolationGroupStatement() {
        this(NodePosition.ZERO);
    }

    public ShowResourceIsolationGroupStatement(NodePosition pos) {
        super(pos);
    }

    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    public List<List<String>> getData() {
        Set<String> allRig = new HashSet<>();
        Map<String, List<String>> rigToFeIds = new HashMap<>();
        for (Frontend fe : GlobalStateMgr.getCurrentState().getNodeMgr().getAllFrontends()) {
            String resourceIsolationGroup = fe.getResourceIsolationGroup();
            allRig.add(resourceIsolationGroup);
            if (!rigToFeIds.containsKey(resourceIsolationGroup)) {
                rigToFeIds.put(resourceIsolationGroup, new ArrayList<>());
            }
            rigToFeIds.get(resourceIsolationGroup).add(fe.getNodeName());
        }
        Map<String, List<String>> rigToCnIds = new HashMap<>();
        for (ComputeNode cn : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getComputeNodes()) {
            String resourceIsolationGroup = cn.getResourceIsolationGroup();
            allRig.add(resourceIsolationGroup);
            if (!rigToCnIds.containsKey(resourceIsolationGroup)) {
                rigToCnIds.put(resourceIsolationGroup, new ArrayList<>());
            }
            rigToCnIds.get(resourceIsolationGroup).add(String.format("%d", cn.getId()));
        }
        List<List<String>> resultRows = new ArrayList<>();
        for (String rig : allRig.stream().sorted().collect(Collectors.toList())) {
            List<String> row = new ArrayList<>();
            row.add(rig);
            if (rigToFeIds.containsKey(rig)) {
                row.add(String.join(",", rigToFeIds.get(rig)));
                row.add(String.format("%d", rigToFeIds.get(rig).size()));
            } else {
                row.add("");
                row.add("");
            }
            if (rigToCnIds.containsKey(rig)) {
                row.add(String.join(",", rigToCnIds.get(rig)));
                row.add(String.format("%d", rigToCnIds.get(rig).size()));
            } else {
                row.add("");
                row.add("");
            }
            resultRows.add(row);
        }
        return resultRows;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowResourceIsolationGroups(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
