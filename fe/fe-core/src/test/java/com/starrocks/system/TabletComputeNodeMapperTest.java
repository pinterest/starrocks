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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/OlapScanNode.java

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

import com.starrocks.catalog.Tablet;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import mockit.Expectations;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

public class TabletComputeNodeMapperTest {
    private Frontend thisFe;
    @Before
    public void setUp() {
        thisFe = new Frontend();
        thisFe.setResourceIsolationGroup(ResourceIsolationGroupUtils.DEFAULT_RESOURCE_ISOLATION_GROUP_ID);
        NodeMgr nodeMgr = GlobalStateMgr.getCurrentState().getNodeMgr();
        new Expectations(nodeMgr) {
            {
                nodeMgr.getMySelf();
                result = thisFe;
                minTimes = 0;
            }
        };
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testGroupManagement() throws Exception {
        TabletComputeNodeMapper mapper = new TabletComputeNodeMapper();
        Assert.assertEquals(0, mapper.numResourceIsolationGroups());

        Long arbitraryTablet = 1000L;

        // Check that the mapper returns the added compute nodes
        mapper.addComputeNode(1L, ResourceIsolationGroupUtils.DEFAULT_RESOURCE_ISOLATION_GROUP_ID);
        Assert.assertEquals(List.of(1L), mapper.computeNodesForTablet(arbitraryTablet));

        mapper.addComputeNode(2L, ResourceIsolationGroupUtils.DEFAULT_RESOURCE_ISOLATION_GROUP_ID);
        // Check that the mapper accurately reports the single resource isolation group.
        Assert.assertEquals(1, mapper.numResourceIsolationGroups());
        // Check that the default number of replicas is 1.
        Assert.assertEquals(1, mapper.computeNodesForTablet(arbitraryTablet).size());

        // check that if we set num replicas to 3 replicas,
        // we get all the nodes in the group as long as num compute nodes in the group is <= 3.
        Assert.assertEquals(2, mapper.computeNodesForTablet(arbitraryTablet, 3).size());

        mapper.addComputeNode(3L, ResourceIsolationGroupUtils.DEFAULT_RESOURCE_ISOLATION_GROUP_ID);
        mapper.addComputeNode(4L, ResourceIsolationGroupUtils.DEFAULT_RESOURCE_ISOLATION_GROUP_ID);
        Assert.assertEquals(3, mapper.computeNodesForTablet(arbitraryTablet, 3).size());



        String otherGroup = "someothergroup";
        mapper.modifyComputeNode(2L, ResourceIsolationGroupUtils.DEFAULT_RESOURCE_ISOLATION_GROUP_ID,
                otherGroup);
        // Check that assigning the compute node to another group is reflected in the count
        Assert.assertEquals(2, mapper.numResourceIsolationGroups());

        // Check that moving the only CN from otherGroup back to the default group again reflects the count correctly
        mapper.modifyComputeNode(2L, otherGroup,
                ResourceIsolationGroupUtils.DEFAULT_RESOURCE_ISOLATION_GROUP_ID);
        Assert.assertEquals(1, mapper.numResourceIsolationGroups());


        // Check that removing the only CN from otherGroup again reflects the count correctly
        mapper.modifyComputeNode(2L, ResourceIsolationGroupUtils.DEFAULT_RESOURCE_ISOLATION_GROUP_ID,
                otherGroup);
        Assert.assertEquals(2, mapper.numResourceIsolationGroups());
        mapper.removeComputeNode(2L, otherGroup);
        Assert.assertEquals(1, mapper.numResourceIsolationGroups());
    }

    @Test
    public void testTabletToCnMapping() throws Exception {
        TabletComputeNodeMapper mapper = new TabletComputeNodeMapper();

        Set<Long> group1Cn = new java.util.HashSet<>(Set.of(0L, 1L, 2L));
        Set<Long> group2Cn = new java.util.HashSet<>(Set.of(3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L));
        int[] cnChoiceCount = new int[group1Cn.size()+group2Cn.size()];

        // Set up mapper for group1 and group2 to have their own CN
        String group1 = "group1id";
        for (Long cnId : group1Cn) {
            mapper.addComputeNode(cnId, group1);
        }
        String group2 = "group2id";
        for (Long cnId : group2Cn) {
            mapper.addComputeNode(cnId, group2);
        }

        Assert.assertEquals(2, mapper.numResourceIsolationGroups());


        int tabletsToTry = 10000;
        long[] tabletIdToGroup2Primary = new long[tabletsToTry];
        long[] tabletIdToGroup2Backup = new long[tabletsToTry];
        for (long tabletId = 0; tabletId < tabletsToTry; tabletId++) {
            // Ensure that mapper chooses cn from right pool for fe in group1
            thisFe.setResourceIsolationGroup(group1);
            List<Long> chosenCn = mapper.computeNodesForTablet(tabletId);
            Assert.assertEquals(1, chosenCn.size());
            Assert.assertTrue(group1Cn.contains(chosenCn.get(0)));
            // Track that the cn has been chosen for later
            cnChoiceCount[chosenCn.get(0).intValue()] += 1;

            // Ensure that mapper chooses cn from right pool for fe in group2
            thisFe.setResourceIsolationGroup(group2);
            chosenCn = mapper.computeNodesForTablet(tabletId, 2);
            // Make sure mapper respects replicas
            Assert.assertTrue(group2Cn.contains(chosenCn.get(0)));
            Assert.assertTrue(group2Cn.contains(chosenCn.get(1)));
            // Track that the cn have been chosen for later
            cnChoiceCount[chosenCn.get(0).intValue()] += 1;
            cnChoiceCount[chosenCn.get(1).intValue()] += 1;
            tabletIdToGroup2Primary[(int) tabletId] = chosenCn.get(0);
            tabletIdToGroup2Backup[(int) tabletId] = chosenCn.get(1);
        }
        // Check that the CN are chosen roughly equally.
        for (long cnId : group1Cn) {
            int chosenCount = cnChoiceCount[(int) cnId];
            float chosenRate = (float) chosenCount / tabletsToTry;
            Assert.assertTrue(chosenRate > .3 && chosenRate < .36);
        }
        for (long cnId : group2Cn) {
            int chosenCount = cnChoiceCount[(int) cnId];
            float chosenRate = (float) chosenCount / tabletsToTry;
            Assert.assertTrue(chosenRate > .16 && chosenRate < .32);
        }

        // Check on remapping behavior after removing some CN
        long cnIdToRemove = 4L;
        Assert.assertTrue(group2Cn.contains(cnIdToRemove));
        mapper.removeComputeNode(cnIdToRemove, group2);
        group2Cn.remove(cnIdToRemove);
        thisFe.setResourceIsolationGroup(group2);
        Map<Long, Integer> backupForRemovedCnToCount = new HashMap<>();
        for (long tabletId = 0; tabletId < tabletsToTry; tabletId++) {
            List<Long> chosenCn = mapper.computeNodesForTablet(tabletId, 2);
            Assert.assertEquals(2, chosenCn.size());
            if (tabletIdToGroup2Primary[(int) tabletId] == cnIdToRemove) {
                Long newPrimary = chosenCn.get(0);
                // If the removed node used to be the primary, check that the old secondary is now the primary
                Assert.assertEquals(tabletIdToGroup2Backup[(int) tabletId], (long) newPrimary);
                Integer occurrenceCount = backupForRemovedCnToCount.containsKey(newPrimary) ?
                        backupForRemovedCnToCount.get(newPrimary) + 1 : 1;
                backupForRemovedCnToCount.put(newPrimary, occurrenceCount);
            } else if (tabletIdToGroup2Backup[(int) tabletId] == cnIdToRemove) {
                // If the removed node used to be the backup, check that the primary is the same and the new secondary is
                // not the removed node.
                Assert.assertEquals(tabletIdToGroup2Primary[(int) tabletId], (long) chosenCn.get(0));
                Assert.assertTrue(chosenCn.get(1) != cnIdToRemove);
            } else {
                // If the removed node was not a replica for the given tablet, check that the mappings haven't changed.
                Assert.assertEquals(tabletIdToGroup2Primary[(int) tabletId], (long) chosenCn.get(0));
                Assert.assertEquals(tabletIdToGroup2Backup[(int) tabletId], (long) chosenCn.get(1));
            }
        }
        // Check that all CN in group2 are being used as backups similarly often (within 1 stddev of average).
        Assert.assertTrue(backupForRemovedCnToCount.size() > 1);
        int cnt = backupForRemovedCnToCount.values().stream().reduce(Integer::sum).get();
        double avg = (double) cnt / backupForRemovedCnToCount.size();
        double stddev =
                backupForRemovedCnToCount.values().stream().mapToDouble(val -> Math.pow(val - avg, 2)).sum();
        for (Integer count : backupForRemovedCnToCount.values()) {
            Assert.assertTrue(Math.abs(count-avg) < stddev);
        }

    }
}