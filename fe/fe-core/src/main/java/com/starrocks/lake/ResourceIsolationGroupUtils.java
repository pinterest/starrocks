package com.starrocks.lake;

import java.util.Objects;

public class ResourceIsolationGroupUtils {
    public static final String DEFAULT_RESOURCE_ISOLATION_GROUP_ID = "";

    public static boolean resourceIsolationGroupMatches(String rig1, String rig2) {
        if (Objects.equals(rig1, rig2)) {
            return true;
        }
        boolean unset1 = rig1 == null || rig1.isEmpty();
        boolean unset2 = rig2 == null || rig2.isEmpty();
        return unset1 && unset2;
    }
}
