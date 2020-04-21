package com.dtc.java.analytic.V1.common.watermarks;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created on 2019-08-21
 *
 * @author :hao.li
 */
public class DTCPeriodicWatermak implements AssignerWithPeriodicWatermarks<String> {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Logger LOGGER = LoggerFactory.getLogger(DTCPeriodicWatermak.class);
    long currentMaxTimestamp = 0L;
    long maxOutOfOrdernes = 3L;
    long lastEmittedWatermark = Long.MIN_VALUE;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        Long potentialWM = currentMaxTimestamp - maxOutOfOrdernes;
        if (potentialWM >= lastEmittedWatermark) {
            lastEmittedWatermark = potentialWM;
        }
        Watermark watermark = new Watermark(potentialWM);
        return watermark;
    }

    @Override
    public long extractTimestamp(String event, long l) {
        JSONObject object = JSON.parseObject(event);
        long time=Long.parseLong(String.valueOf(object.get("clock")));
//        long time = event.getClock();
        currentMaxTimestamp = Math.max(time, currentMaxTimestamp);
        System.out.println("eventtime:" + sdf.format(new Date(Long.valueOf(time +"000")))+ "     " + "currentMaxTimestamp:" + sdf.format(new Date(Long.valueOf(getCurrentWatermark().getTimestamp()+"000"))));
//        System.out.println("eventtime1:" + time + "     " + "currentMaxTimestamp1:" + getCurrentWatermark().getTimestamp());
        return time;
    }

    private Long getTime(String str) {
        String s = str.replaceAll("[A-Z]", " ");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = simpleDateFormat.parse(s);
        } catch (ParseException e) {
            LOGGER.warn("Data parse is mistake,and reasson is {}.", e);
        }
        long time = date.getTime();
        return time;
    }
}
