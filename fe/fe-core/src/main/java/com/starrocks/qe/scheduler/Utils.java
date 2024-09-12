package com.starrocks.qe.scheduler;

import com.starrocks.thrift.TScanRange;

import java.util.Optional;

public class Utils {
    public static Optional<Long> maybeGetTabletId(TScanRange scanRange) {
        Optional<Long> optTabletId = Optional.empty();
        if (scanRange.internal_scan_range != null) {
            optTabletId = Optional.of(scanRange.internal_scan_range.tablet_id);
        }
        return optTabletId;
    }
}
