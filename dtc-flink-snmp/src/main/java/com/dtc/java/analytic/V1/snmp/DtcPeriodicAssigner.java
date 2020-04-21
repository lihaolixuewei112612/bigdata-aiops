package com.dtc.java.analytic.V1.snmp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;

public class DtcPeriodicAssigner implements  AssignerWithPeriodicWatermarks<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DtcPeriodicAssigner.class);

    private long maxOutOfOrderness = 2000; // 2 seconds

    private long currentMaxTimestamp = 0L;


    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        Watermark watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        return watermark;

    }

    @Override
    public long extractTimestamp(String s, long l) {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode json = null;
        try {
            json = objectMapper.readTree(s);
            String time = json.get("time").textValue();
            long timestamp =  Long.parseLong(time);
            return timestamp;
        } catch (IOException e) {
            LOGGER.warn("Data parse is mistake,and reasson is {}.", e);
        }
        return 0;
    }
}
