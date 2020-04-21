//package com.dtc.java.analytic.common.utils;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import com.dtc.java.analytic.common.model.MetricEvent;
//import com.dtc.java.analytic.common.watermarks.DTCPeriodicWatermak;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.tuple.Tuple;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple6;
//import org.apache.flink.api.java.tuple.Tuple7;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.datastream.WindowedStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
//import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.util.Collector;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;
//
///**
// * Created on 2019-10-17
// *
// * @author :hao.li
// */
//public class WaterTimeUtil {
//
//    public static SingleOutputStreamOperator
//    buildWaterMarks(StreamExecutionEnvironment env, DataStreamSource<String> streamSource) {
//        ParameterTool parameter = (ParameterTool) env.getConfig().getGlobalJobParameters();
//        long windowSizeMillis = Long.parseLong(parameter.get("windowSizeMillis", "4000"));
//        SingleOutputStreamOperator<String> metricEventSingleOutputStreamOperator = streamSource.assignTimestampsAndWatermarks(new DTCPeriodicWatermak());
//        WindowedStream<Tuple2<String, Tuple6<String, String, String, String, Long, String>>, Tuple, TimeWindow> tuple2TupleTimeWindowWindowedStream = metricEventSingleOutputStreamOperator
//                .map(new DTCZabbixMapFunction()).filter(e->(!e.f1.f3.equals("字符串类型")))
//                .keyBy(0)
//                .timeWindow(Time.milliseconds(5));
//        SingleOutputStreamOperator<Tuple7<String, String, String, String, String, Long, String>> apply = tuple2TupleTimeWindowWindowedStream
//                .apply(new DtcApplyWindowFunction());
//        return apply;
//    }
//
//}
//
//class DTCZabbixMapFunction implements MapFunction<String, Tuple2<String, Tuple6<String, String, String, String, Long, String>>> {
//
//    @Override
//    public Tuple2<String, Tuple6<String, String, String, String, Long, String>> map(String event) {
//        CountUtils.incrementEventReceivedCount();
//        Tuple2<String, Tuple6<String, String, String, String, Long, String>> message;
//        JSONObject object = JSON.parseObject(event);
//        String ip = object.getString("ip");
//        String host = object.getString("host");
//        String oid = object.getString("oid");
//        String name = object.getString("name");
//        String english = object.getString("english");
//        long clock = object.getLong("clock");
//        String value = object.getString("value");
//        boolean b = strIsNum(value);
//        if(b) {
//            message = Tuple2.of(ip, Tuple6.of(host, oid, name, english, clock, value));
//        }else {
//            message = Tuple2.of(ip, Tuple6.of(host, oid, name, "字符串类型", clock, value));
//        }
//        return message;
//    }
//
//    private boolean strIsNum(String str) {
//        try {
//            Double.parseDouble(str);
//            return true;
//        } catch (NumberFormatException e) {
//            return false;
//        }
//    }
//}
//
//class DtcApplyWindowFunction implements WindowFunction<Tuple2<String, Tuple6<String, String, String, String, Long, String>>, Tuple7<String, String, String, String, String, Long, String>, Tuple, TimeWindow> {
//
//    @Override
//    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Tuple6<String, String, String, String, Long, String>>> iterable, Collector<Tuple7<String, String, String, String, String, Long, String>> collector) throws Exception {
//        Map<String, Map<String, Double>> map = new HashMap<>();
//        for (Tuple2<String, Tuple6<String, String, String, String, Long, String>> in : iterable) {
//
//            Map<String, Double> lihao = new HashMap<>();
//            if (in.f1.f1.equals(".1.3.6.1.4.1.2021.10.1.3.1")) {
//                if (map.containsKey(in.f0) && map.get(in.f0).containsKey(".1.3.6.1.4.1.2021.10.1.3.2")) {
//                    double a = Double.parseDouble(in.f1.f5) / map.get(in.f0).get(".1.3.6.1.4.1.2021.10.1.3.2");
//                    collector.collect(Tuple7.of(in.f0, in.f1.f0, in.f1.f1, in.f1.f2, in.f1.f3, in.f1.f4, String.valueOf(a)));
//                    map.remove("10.3.7.232");
//                    continue;
//                } else {
//                    lihao.put(in.f1.f1, Double.parseDouble(in.f1.f5));
//                    map.put(in.f0, lihao);
//                }
//            } else if (in.f1.f1.equals(".1.3.6.1.4.1.2021.10.1.3.2")) {
//                if (map.containsKey(in.f0) && map.get(in.f0).containsKey(".1.3.6.1.4.1.2021.10.1.3.1")) {
//                    double a = Double.parseDouble(in.f1.f5 )/ map.get(in.f0).get(".1.3.6.1.4.1.2021.10.1.3.1");
//                    collector.collect(Tuple7.of(in.f0, in.f1.f0, in.f1.f1, in.f1.f2, in.f1.f3, in.f1.f4, String.valueOf(a)));
//                    map.remove("10.3.7.232");
//                    continue;
//                } else {
//                    lihao.put(in.f1.f1, Double.parseDouble(in.f1.f5));
//                    map.put(in.f0, lihao);
//                }
//            }
//        }
//        System.out.println("lihao-----------------------");
//        Tuple2<String, Tuple6<String, String, String, String, Long, String>> in = iterable.iterator().next();
//        String format = String.format("data:   %s  startTime:  %s    Endtim:  %s", in.toString(), timeWindow.getStart(), timeWindow.getEnd());
//        System.out.println(format);
//        collector.collect(Tuple7.of(in.f0, in.f1.f0, in.f1.f1, in.f1.f2, in.f1.f3, in.f1.f4, in.f1.f5));
//    }
//}
//
////class DtcApplyWindowFunction implements WindowFunction<Tuple2<String, Tuple6<String, String, String, String, Long, Double>>, Tuple7<String, String, String, String, String, Long, Double>, Tuple, TimeWindow> {
////
////    @Override
////    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Tuple6<String, String, String, String, Long, Double>>> iterable, Collector<Tuple7<String, String, String, String, String, Long, Double>> collector) throws Exception {
////        System.out.println("lihao-----------------------");
////        Tuple2<String, Tuple6<String, String, String, String, Long, Double>> in = iterable.iterator().next();
////        String format = String.format("data:   %s  startTime:  %s    Endtim:  %s", in.toString(), timeWindow.getStart(), timeWindow.getEnd());
////        System.out.println(format);
////        collector.collect(Tuple7.of(in.f0, in.f1.f0, in.f1.f1, in.f1.f2, in.f1.f3, in.f1.f4, in.f1.f5));
////    }
////}
//
//
