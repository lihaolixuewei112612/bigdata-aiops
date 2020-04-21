package com.dtc.java.analytic.V1.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created on 2019-08-16
 *
 * @author :hao.li
 */
public class MySourceEvent extends RichSourceFunction<String> {
    private boolean running = true;
    Random rand = new Random();
    List channel = Arrays.asList("a", "b", "c", "d");

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Long numElements = Long.MAX_VALUE;
        Long count = 0L;
        while (running && count < numElements) {
            String str = "{\"labels\":{\"__name__\":\"oracledb_resource_current_utilization\",\"instance\":\"10.3.7.231:9161\",\"job\":\"consul\",\"resource_name\":\"gcs_shadows\"},\"name\":\"oracledb_resource_current_utilization\",\"timestamp\":\"2019-08-23T06:40:52Z\",\"value\":\"0\"}";
//            String str = generateEvent();
            ctx.collect(str);
        }

    }
    private String generateEvent(){
        String s = readableDate();
        String str = channel.get(rand.nextInt(4)).toString();
        int value = rand.nextInt(9);
        int next = rand.nextInt(2) + 1;
        int result = rand.nextInt(30);
        int test1 = rand.nextInt(3) + 5;
        int test2 = rand.nextInt(4) + 6;

        String message = "{" + "\"code\"" + ":" + "\"" + 101 + "|" + 101 + "|" + 101 + "_" + 10 + next + "|" + 10 + test1 + "|" + 10 + test2 + "\"" + "," + "\"host\"" + ":" + "\"" + str +
                "\"" + "," + "\"" + "time" + "\"" + ":" + "\"" + s + "\"" + "," + "\"" + "value" + "\"" + ":" +
                "\"" + result + "\"" + "}";
        return message;
    }
    private String readableDate(){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long l = System.nanoTime();
        Date date = new Date(l);
        String format = simpleDateFormat.format(date);
        return format;
    }

    @Override
    public void cancel() {
        running = false;

    }
}
