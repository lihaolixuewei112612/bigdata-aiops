package com.dtc.java.analytic.V2.worker;

import com.alibaba.fastjson.JSON;
import com.dtc.java.analytic.V2.common.model.SourceEvent;
import org.apache.flink.api.java.tuple.Tuple9;
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
public class TestSourceEvent extends RichSourceFunction<Tuple9<String, String, String, String, Double, String, String, String, String>> {
    private boolean running = true;
    Random rand = new Random();
    //(601|1|99,10.3.7.231,指标告警,1分钟平均load值,0.0,101_101_101_101_101,1,test,test12)
    List<String> channel = Arrays.asList("10.3.7.231", "10.3.7.232", "10.3.7.233", "10.3.7.234");

    @Override
    public void run(SourceContext<Tuple9<String, String, String, String, Double, String, String, String, String>> ctx) throws Exception {
        Long numElements = Long.MAX_VALUE;
        Long count = 0L;

        while (running && count < numElements) {
            Tuple9<String, String, String, String, Double, String, String, String, String> s = generateEvent();
            ctx.collect(s);
            Thread.sleep(100);
        }

    }
    private Tuple9<String, String, String, String, Double, String, String, String, String> generateEvent(){
        Tuple9<String, String, String, String, Double, String, String, String, String> tuple9=new Tuple9<>();
        tuple9.f0=rand.nextInt(100)+"|"+rand.nextInt(10)+"|"+rand.nextInt(100);
        tuple9.f1 = channel.get(rand.nextInt(channel.size()));
        tuple9.f2="指标告警";
        tuple9.f3="1分钟平均load值";
        tuple9.f4=0.0;
        tuple9.f5="101_100_101_101_101";
        tuple9.f6="1";
        tuple9.f7="5";
        tuple9.f8="2";
        return tuple9;
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
