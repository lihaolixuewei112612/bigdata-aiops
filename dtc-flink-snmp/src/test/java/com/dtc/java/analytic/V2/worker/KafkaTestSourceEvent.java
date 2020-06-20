package com.dtc.java.analytic.V2.worker;

import com.alibaba.fastjson.JSON;
import com.dtc.java.analytic.V2.common.model.SourceEvent;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Created on 2019-08-16
 *
 * @author :hao.li
 */
public class KafkaTestSourceEvent extends RichSourceFunction<SourceEvent> {
    private boolean running = true;
    Random rand = new Random();
    List<String> channel = Arrays.asList("10.3.7.231", "10.3.7.232", "10.3.7.233", "10.3.7.234");
    List<String> channel1 = Arrays.asList("0.8", "0.7", "0.9", "1.7");
    //{"time":"1589537380872","code":"101_101_101_101_101","host":"10.3.7.232","nameCN":"1分钟平均load值","value":"0.23","nameEN":"system_load1"}
//{"time":"1589537382872","code":"101_101_101_101_101","host":"10.3.7.232","nameCN":"1分钟平均load值","value":"0.29","nameEN":"system_load1"}
//{"time":"1589537380871","code":"101_101_101_101_101","host":"10.3.7.231","nameCN":"1分钟平均load值","value":"0.8","nameEN":"system_load1"}
//{"time":"1589537382871","code":"101_101_101_101_101","host":"10.3.7.231","nameCN":"1分钟平均load值","value":"0.5","nameEN":"system_load1"}
    @Override
    public void run(SourceContext<SourceEvent> ctx) throws Exception {
        Long numElements = Long.MAX_VALUE;
        Long count = 0L;
        while (running && count < numElements) {
            SourceEvent s = generateEvent();
            String str = "{\"labels\":{\"__name__\":\"oracledb_resource_current_utilization\",\"instance\":\"10.3.7.231:9161\",\"job\":\"consul\",\"resource_name\":\"gcs_shadows\"},\"name\":\"oracledb_resource_current_utilization\",\"timestamp\":\"2019-08-23T06:40:52Z\",\"value\":\"0\"}";
//            String str = generateEvent();
            ctx.collect(s);
            Thread.sleep(2000);
        }

    }
    private SourceEvent generateEvent(){
        SourceEvent event = new SourceEvent();
        event.setTime("1589537380872");
        event.setCode("101_100_101_101_101");
        event.setHost(channel.get(rand.nextInt(channel.size())));
        event.setNameCN("1分钟平均load值");
        event.setNameEN("system_load1");
        event.setValue(channel1.get(rand.nextInt(channel1.size())));
        return event;
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
