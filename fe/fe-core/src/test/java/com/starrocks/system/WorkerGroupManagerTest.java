package com.starrocks.system;

import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

import static com.starrocks.lake.StarOSAgent.DEFAULT_WORKER_GROUP_ID;
import static com.starrocks.system.ResourceIsolationGroupUtils.DEFAULT_RESOURCE_ISOLATION_GROUP_ID;

public class WorkerGroupManagerTest {
    @Test
    public void testBasic(@Mocked GlobalStateMgr globalStateMgr, @Mocked StarOSAgent starOsAgent) throws Exception {
        String rig1 = "some_resource_isolation_group";
        Long wgid1 = 1L;
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getStarOSAgent();
                minTimes = 0;
                result = starOsAgent;
            }
        };
        new Expectations(starOsAgent) {
            {

                starOsAgent.tryGetWorkerGroupForOwner(rig1);
                result = Optional.empty();
                times = 1;

                // getOrCreateWorkerGroupForOwner should only be called once
                starOsAgent.getOrCreateWorkerGroupForOwner(rig1);
                result = wgid1;
                times = 1;
            }
        };

        WorkerGroupManager workerGroupManager = new WorkerGroupManager();
        Assert.assertEquals(DEFAULT_WORKER_GROUP_ID,
                workerGroupManager.getWorkerGroup(DEFAULT_RESOURCE_ISOLATION_GROUP_ID).get().longValue());
        Assert.assertEquals(DEFAULT_WORKER_GROUP_ID,
                workerGroupManager.getOrCreateWorkerGroup(DEFAULT_RESOURCE_ISOLATION_GROUP_ID).longValue());
        Assert.assertEquals(Optional.empty(), workerGroupManager.getWorkerGroup(rig1));
        Assert.assertEquals(wgid1, workerGroupManager.getOrCreateWorkerGroup(rig1));
        Assert.assertEquals(wgid1, workerGroupManager.getOrCreateWorkerGroup(rig1));
        Assert.assertEquals(Optional.of(wgid1), workerGroupManager.getWorkerGroup(rig1));

    }

}