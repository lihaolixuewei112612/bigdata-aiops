package com.dtc.java.analytic.V1.alter;

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
public class MySourceEvent extends RichSourceFunction<String> {
    private boolean running = true;
    Random rand = new Random();
    List channel = Arrays.asList("1.2.3.4.5", "1.2.3.4.6", "1.2.3.4.7", "1.2.3.4.8");

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Long numElements = Long.MAX_VALUE;
        Long count = 0L;
        while (running && count < numElements) {
            String s = generateEvent();
            String str = "{\"labels\":{\"__name__\":\"oracledb_resource_current_utilization\",\"instance\":\"10.3.7.231:9161\",\"job\":\"consul\",\"resource_name\":\"gcs_shadows\"},\"name\":\"oracledb_resource_current_utilization\",\"timestamp\":\"2019-08-23T06:40:52Z\",\"value\":\"0\"}";
//            String str = generateEvent();
            ctx.collect(s);
            Thread.sleep(10);
        }

    }
    private String generateEvent(){
        String s = readableDate();
        String str = channel.get(rand.nextInt(4)).toString();
        int value = rand.nextInt(9);
        int next = rand.nextInt(3);
        int result = rand.nextInt(100);
        int test2 = rand.nextInt(9);


        //{"time":"1581691002687","code":"101_101_107_105_105","host":"10.3.7.234","nameCN":"磁盘剩余大小","value":"217802544","nameEN":"disk_free"}


        String message = "{" + "\"code\"" + ":" + "\"" + 101 + "_" + 101 + "_" + 101 + "_" + 10 + next + "_" + 10 + next + "\"" + "," + "\"host\"" + ":" + "\"" + str +
                "\"" + "," + "\"" + "time" + "\"" + ":" + "\"" + s + "\"" + "," + "\"" + "value" + "\"" + ":" +
                "\"" + result + "\"" +","+"\""+"\"nameCN\""+":"+"\""+"磁盘剩余大小"+"\""+","+"\""+"nameEN"+"\""+":"+"\""+"disk_free"+"\""+"}";
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
