package com.dtc.java.analytic.V1.snmp;

import com.dtc.java.analytic.V1.source.MySourceEvent;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created on 2019-08-12
 *
 * @author :hao.li
 */
public class StreamToFlink {
    private static final Logger logger = LoggerFactory.getLogger(StreamToFlink.class);

    public static void main(String[] args) throws Exception {
        int windowSizeMillis = 5000;
        int windowSlideMillis = 5000;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(60000);
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.addSource(new MySourceEvent());
        WindowedStream<Tuple4<Tuple3<String, String, String>, String, String, Double>, Tuple, TimeWindow>
                tuple4TupleTimeWindowWindowedStream = dataStreamSource
                .map(new MyMapFunction())
                .keyBy(0)
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS),
                        Time.of(windowSlideMillis, TimeUnit.MILLISECONDS));
        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, Double>> process
                = tuple4TupleTimeWindowWindowedStream.process(new myProcessWindowFunction());
        String str = "http://10.3.0.170:4242";
        process.addSink(new SinkToOpentsdb(str));
        env.execute("start");
    }
}

//class MyFlatMapFunction implements FlatMapFunction<String, DataStruct> {
//    public void flatMap(String s, Collector<DataStruct> collector) throws Exception {
//        DataStruct message;
//        ObjectMapper objectMapper = new ObjectMapper();
//        JsonNode json = objectMapper.readTree(s);
//        String[] codes = json.get("code").toString().split("\\|");
//        if (codes.length == 5) {
//            String system_name = codes[0].trim() + "_" + codes[1].trim();
//            String ZB_name = system_name + "_" + codes[2].trim() + "_" + codes[3].trim();
//            String ZB_code = codes[4].trim();
//            String time = json.get("time").toString();
//            Double value = json.get("value").asDouble();
//            String host = json.get("host").toString().trim();
//            message = new DataStruct(system_name, host, ZB_name, ZB_code, time, value);
//            collector.collect(message);
//        }
//    }
//}

class myProcessWindowFunction extends ProcessWindowFunction<Tuple4<Tuple3<String, String, String>, String, String,
        Double>, Tuple6<String, String, String, String, String, Double>, Tuple, TimeWindow> {

    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple4<Tuple3<String, String, String>, String, String,
            Double>> elements, Collector<Tuple6<String, String, String, String, String, Double>> collector)
            throws Exception {
        Map map = new HashMap<String, Double>();
        for (Tuple4<Tuple3<String, String, String>, String, String, Double> in : elements) {
            if (getMessage_Two(collector, map, in, "101_101_101_101_101", "101", "102")) continue;
            if (getMessage_Two(collector, map, in, "101_101_101_101_105", "106", "107")) continue;
            if (getMessage_Two(collector, map, in, "101_101_101_105_122", "125", "126")) continue;
            if (getMessage_Two(collector, map, in, "101_101_101_106_124", "129", "130")) continue;
            if (getMessage_Two(collector, map, in, "101_101_101_106_125", "131", "132")) continue;
            if (in.f0.f2.contains("101_101_101_102_108")) {
                if (in.f1.equals("110"))
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f0.f2, in.f1, in.f2, 1 - in.f3));
                continue;
            }
            if (getMessage_Three(collector, map, in, "101_101_101_104_118", "117", "118", "119")) continue;
            if (getMessage_Three(collector, map, in, "101_101_101_104_119", "120", "121", "122")) continue;

            if (in.f0.f2.contains("101_101_101_105_123")) {
                if (in.f1.equals("127")) {
                    map.put("127", in.f3);
                    if (map.containsKey("128")) {
                        double send_IPv4_All = Double.parseDouble(map.get("127").toString());
                        double send_IPv4_Lost = Double.parseDouble(map.get("128").toString());
                        double result = 1 - (send_IPv4_Lost / send_IPv4_All);
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f0.f2, "000", in.f2, result));
                        map.remove("127");
                        map.remove("128");
                        continue;
                    } else {
                        continue;
                    }
                }
                if (in.f1.equals("128")) {
                    map.put("128", in.f3);
                    if (map.containsKey("127")) {
                        double send_IPv4_All = Double.parseDouble(map.get("127").toString());
                        double send_IPv4_Lost = Double.parseDouble(map.get("128").toString());
                        double result = 1 - (send_IPv4_Lost / send_IPv4_All);
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f0.f2, "000", in.f2, result));
                        map.remove("127");
                        map.remove("128");
                        continue;
                    } else {
                        continue;
                    }
                }
            }

            collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f0.f2, in.f1, in.f2, in.f3));
        }
    }

    private boolean getMessage_Three(Collector<Tuple6<String, String, String, String, String, Double>> collector, Map map, Tuple4<Tuple3<String, String, String>, String, String, Double> in, String s, String s2, String s3, String s4) {
        if (in.f0.f2.contains(s)) {
            if (in.f1.equals(s2)) {
                map.put(s2, in.f3);
                if (map.containsKey(s3) && map.containsKey(s4)) {
                    double rPackageSingle = Double.parseDouble(map.get(s2).toString());
                    double rPackageDouble = Double.parseDouble(map.get(s3).toString());
                    double rLostPackageNum = Double.parseDouble(map.get(s4).toString());
                    double result = rLostPackageNum / (rPackageSingle + rPackageDouble);
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f0.f2, "000", in.f2, result));
                    map.remove(s2);
                    map.remove(s3);
                    map.remove(s4);
                    return true;
                } else {
                    return true;
                }
            }
            if (in.f1.equals(s3)) {
                map.put(s3, in.f3);
                if (map.containsKey(s2) && map.containsKey(s4)) {
                    double rPackageSingle = Double.parseDouble(map.get(s2).toString());
                    double rPackageDouble = Double.parseDouble(map.get(s3).toString());
                    double rLostPackageNum = Double.parseDouble(map.get(s4).toString());
                    double result = rLostPackageNum / (rPackageSingle + rPackageDouble);
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f0.f2, "000", in.f2, result));
                    map.remove(s2);
                    map.remove(s3);
                    map.remove(s4);
                    return true;
                } else {
                    return true;
                }
            }
            if (in.f1.equals(s4)) {
                map.put(s4, in.f3);
                if (map.containsKey(s2) && map.containsKey(s3)) {
                    double rPackageSingle = Double.parseDouble(map.get(s2).toString());
                    double rPackageDouble = Double.parseDouble(map.get(s3).toString());
                    double rLostPackageNum = Double.parseDouble(map.get(s4).toString());
                    double result = rLostPackageNum / (rPackageSingle + rPackageDouble);
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f0.f2, "000", in.f2, result));
                    map.remove(s2);
                    map.remove(s3);
                    map.remove(s4);
                    return true;
                } else {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean getMessage_Two(Collector<Tuple6<String, String, String, String, String, Double>> collector, Map map, Tuple4<Tuple3<String, String, String>, String, String, Double> in, String s, String s2, String s3) {
        if (in.f0.f2.contains(s)) {
            if (in.f1.equals(s2)) {
                map.put(s2, in.f3);
                if (map.containsKey(s3)) {
                    double memory_All = Double.parseDouble(map.get(s2).toString());
                    double memory_Used = Double.parseDouble(map.get(s3).toString());
                    double result = memory_Used / memory_All;
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f0.f2, "000", in.f2, result));
                    map.remove(s2);
                    map.remove(s3);
                    return true;
                } else {
                    return true;
                }
            }
            if (in.f1.equals(s3)) {
                map.put(s3, in.f3);
                if (map.containsKey(s2)) {
                    double memory_All = Double.parseDouble(map.get(s2).toString());
                    double memory_Used = Double.parseDouble(map.get(s3).toString());
                    double result = memory_Used / memory_All;
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f0.f2, "000", in.f2, result));
                    map.remove(s2);
                    map.remove(s3);
                    return true;
                } else {
                    return true;
                }
            }
        }
        return false;
    }
}
