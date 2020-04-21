package com.dtc.java.analytic.V1.common.watermarks;

import com.dtc.java.analytic.V1.snmp.DataStruct;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class DtcPeriodicAssigner implements AssignerWithPeriodicWatermarks<DataStruct> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DtcPeriodicAssigner.class);

    private long maxOutOfOrderness = 2000; // 2 seconds

    private long currentMaxTimestamp = 0L;

    long lastEmittedWatermark = Long.MIN_VALUE;


    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        Long potentialWM = currentMaxTimestamp - maxOutOfOrderness;
        if (potentialWM >= lastEmittedWatermark) {
            lastEmittedWatermark = potentialWM;
        }
        Watermark watermark = new Watermark(potentialWM);
        return watermark;
    }

    @Override
    public long extractTimestamp(DataStruct s, long l) {
        long timestamp = Long.parseLong(s.getTime());
        return timestamp;
    }
}
