package com.dtc.java.analytic.V1.snmp;

import com.dtc.java.analytic.V1.configuration.Configuration;
import com.dtc.java.analytic.V1.configuration.NumberTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created on 2019-08-12
 *
 * @author :ren
 */
public class StreamToFlink {
    public static final Logger logger = LoggerFactory.getLogger(StreamToFlink.class);

    //创建存储历史数据的map
    public static Map totalMap = new HashMap<String,Double>();

    public static void main(String[] args) throws Exception {
        int windowSizeMillis = 4000;
        //int windowTumblingMillis = 3000;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //Flink Streaming检查点（Checkpointing）设置
        env.enableCheckpointing(60000);
        env.setParallelism(2);

        //读取kafka数据源的配置文件
        Properties prop = Configuration.getConf("kafka-flink_test.properties");
        String TOPIC = prop.get("topic").toString();
        //从kafka消费并获取数据流
        FlinkKafkaConsumer myConsumer = new FlinkKafkaConsumer(TOPIC, new SimpleStringSchema(), prop);
        //从最新的数据消费
        myConsumer.setStartFromLatest();
        //设置水印
        myConsumer.assignTimestampsAndWatermarks(new DtcPeriodicAssigner());
        DataStreamSource<String> dataStreamSource = env.addSource(myConsumer);
        WindowedStream<Tuple5<Tuple2<String, String>, String, String, String, String>, Tuple, TimeWindow>
                tuple5TupleTimeWindowWindowedStream = dataStreamSource
                .map(new MyMapFunction())
                .keyBy(0)
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS));
        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, String>> process
                = tuple5TupleTimeWindowWindowedStream.process(new myProcessWindowFunction());
        //process.print();
        String str = "http://10.3.7.232:4399";

        process.addSink(new SinkToOpentsdb(str));
        env.execute("Test-start");

    }
}


class MyMapFunction implements MapFunction<String, Tuple5<Tuple2<String, String>, String, String, String, String >> {

    //对json数据进行解析并且存入Tuple
    @Override
    public Tuple5<Tuple2<String, String>, String, String, String, String > map(String s) throws Exception {
        Tuple5<Tuple2<String, String>, String, String, String, String > abc = null;
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode json = objectMapper.readTree(s);
        String[] codes = json.get("code").textValue().split("_");
        String system_name = codes[0].trim()+"_"+codes[1].trim() ;
        String ZB_name = codes[0].trim()+"_"+codes[1].trim() + "_" + codes[2].trim() + "_" + codes[3].trim();
        String ZB_code = codes[4].trim();
        String time = json.get("time").textValue();
        String value = json.get("value").textValue();
        String host = json.get("host").textValue().trim();
        abc = Tuple5.of(Tuple2.of(system_name, host), ZB_name, ZB_code, time, value);
        //System.out.println(ZB_name+"@@@"+host+"@@@"+time+"@@@"+value);
        return abc;

    }
}


class myProcessWindowFunction extends ProcessWindowFunction<Tuple5<Tuple2<String, String>, String, String, String,
        String>, Tuple6<String, String, String, String, String, String>, Tuple, TimeWindow> {

    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple5<Tuple2<String, String>, String, String, String,
            String>> elements, Collector<Tuple6<String, String, String, String, String, String>> collector)
            throws Exception {

        Map map = new HashMap<String, String>();
        //网络接口数量
        int netCardNumber = 0;
        //网卡发包总数的数组
        Map net_bytes_sent = new HashMap<String, String>();
        //网卡发包总数的数组
        Map net_bytes_recv = new HashMap<String, String>();
        //网卡收包错误数量的数组
        Map net_err_in = new HashMap<String, String>();
        //网卡发包错误数量的数组
        Map net_err_out = new HashMap<String, String>();
        //网卡收 丢包数量的数组
        Map net_drop_in = new HashMap<String, String>();
        //网卡发 丢包数量的数组
        Map net_drop_out = new HashMap<String, String>();
        //网卡发包数量的数组
        Map net_packets_sent = new HashMap<String, String>();
        //网卡发包数量的数组
        Map net_packets_recv = new HashMap<String, String>();

        for (Tuple5<Tuple2<String, String>, String, String, String, String> in : elements) {
            //判断是否是数据
            if (!NumberTest.isNumber(in.f4)) {
                continue;
            }else{

            /*
            主机系统参数
             */

            //系统启动时间:{"time":"1577100221289","code":"101_101_101_106_106","host":"10.3.7.233","nameCN":"系统启动时间","value":"94 days, 8:45:28.10","nameEN":"system_uptime"}
            if ("101_101_101_106".equals(in.f1)) {
                String day = in.f4.split(",")[0];
                String result = day.split("\\s+")[0];
                collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, in.f2, in.f3, result));
                continue;
            }


        /*
        主机网络接口状态
         */
            //获取网络接口数量
            if ("101_101_101_109".equals(in.f1)) {
                netCardNumber = Integer.parseInt(in.f4);
                collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, in.f2, in.f3, in.f4));
                continue;
            }

            //网卡发包总数(M)
            if (in.f1.equals("101_101_103_101")) {
                if (net_bytes_sent.size() != netCardNumber || netCardNumber == 0) {
                    net_bytes_sent.put(in.f1 + "_" + in.f2, in.f4);
                    continue;
                } else {
                    Set<String> set = net_bytes_sent.keySet();
                    double result = 0;
                    for (String key : set) {
                        result += Double.parseDouble(net_bytes_sent.get(key).toString());
                    }
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result / 1048576 + ""));
                    net_bytes_sent.clear();
                    continue;
                }
            }
            //网卡收包总数(M)
            if (in.f1.equals("101_101_103_103")) {
                if (net_bytes_recv.size() != netCardNumber || netCardNumber == 0) {
                    net_bytes_recv.put(in.f1 + "_" + in.f2, in.f4);
                    continue;
                } else {
                    Set<String> set = net_bytes_recv.keySet();
                    double result = 0;
                    for (String key : set) {
                        result += Double.parseDouble(net_bytes_recv.get(key).toString());
                    }
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result / 1048576 + ""));
                    net_bytes_recv.clear();
                    continue;
                }
            }
            //网卡发包总数量（个）
            if (in.f1.equals("101_101_103_105")) {
                if (net_packets_sent.size() != netCardNumber * 2 || netCardNumber == 0) {
                    net_packets_sent.put(in.f1 + "_" + in.f2, in.f4);
                    continue;
                } else {
                    Set<String> set = net_packets_sent.keySet();
                    double result = 0;
                    for (String key : set) {
                        result += Double.parseDouble(net_packets_sent.get(key).toString());
                    }
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result / 1048576 + ""));
                    net_packets_sent.clear();
                    continue;
                }
            }
            //网卡发包总数量（个）
            if (in.f1.equals("101_101_103_106")) {
                if (net_packets_recv.size() != netCardNumber * 2 || netCardNumber == 0) {
                    net_packets_recv.put(in.f1 + "_" + in.f2, in.f4);
                    continue;
                } else {
                    Set<String> set = net_packets_recv.keySet();
                    double result = 0;
                    for (String key : set) {
                        result += Double.parseDouble(net_packets_recv.get(key).toString());
                    }
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result + ""));
                    net_packets_recv.clear();
                    continue;
                }
            }
            //网卡收包错误数量（个）
            if (in.f1.equals("101_101_103_107")) {
                if (net_err_in.size() != netCardNumber || netCardNumber == 0) {
                    net_err_in.put(in.f1 + "_" + in.f2, in.f4);
                    continue;
                } else {
                    Set<String> set = net_err_in.keySet();
                    double result = 0;
                    for (String key : set) {
                        result += Double.parseDouble(net_err_in.get(key).toString());
                    }
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result + ""));
                    net_err_in.clear();
                    continue;
                }
            }
            //网卡发包错误数量（个）
            if (in.f1.equals("101_101_103_108")) {
                if (net_err_out.size() != netCardNumber || netCardNumber == 0) {
                    net_err_out.put(in.f1 + "_" + in.f2, in.f4);
                    continue;
                } else {
                    Set<String> set = net_err_out.keySet();
                    double result = 0;
                    for (String key : set) {
                        result += Double.parseDouble(net_err_out.get(key).toString());
                    }
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result + ""));
                    net_err_out.clear();
                    continue;
                }
            }
            //网卡收 丢包数量（个）
            if (in.f1.equals("101_101_103_109")) {
                if (net_drop_in.size() != netCardNumber || netCardNumber == 0) {
                    net_drop_in.put(in.f1 + "_" + in.f2, in.f4);
                    continue;
                } else {
                    Set<String> set = net_drop_in.keySet();
                    double result = 0;
                    for (String key : set) {
                        result += Double.parseDouble(net_drop_in.get(key).toString());
                    }
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result + ""));
                    net_drop_in.clear();
                    continue;
                }
            }
            //网卡发 丢包数量（个）
            if (in.f1.equals("101_101_103_110")) {
                if (net_drop_out.size() != netCardNumber || netCardNumber == 0) {
                    net_drop_out.put(in.f1 + "_" + in.f2, in.f4);
                    continue;
                } else {
                    Set<String> set = net_drop_out.keySet();
                    double result = 0;
                    for (String key : set) {
                        result += Double.parseDouble(net_drop_out.get(key).toString());
                    }
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result + ""));
                    net_drop_in.clear();
                    continue;
                }
            }

            /*
                主机内存
             */
            //Linux内存空闲率
                if ("101_101_104_110".equals(in.f1)) {
                    if (in.f2.equals("113")) {
                        map.put("101_101_104_109_113", in.f4);
                        if (map.containsKey("101_101_104_109_114") && map.containsKey("101_101_104_109_120") && map.containsKey("101_101_104_109_121")) {
                            double mem_total = Double.parseDouble(map.get("101_101_104_109_113").toString());
                            double mem_available = Double.parseDouble(map.get("101_101_104_109_114").toString());
                            double mem_buffered = Double.parseDouble(map.get("101_101_104_109_120").toString());
                            double mem_cached = Double.parseDouble(map.get("101_101_104_109_121").toString());
                            String result = (1 -(mem_total-mem_available-mem_buffered-mem_cached) / mem_total) * 100 + "";
                            collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                            map.remove("101_101_104_109_113");
                            map.remove("101_101_104_109_114");
                            map.remove("101_101_104_109_120");
                            map.remove("101_101_104_109_121");
                            continue;
                        } else {
                            continue;
                        }
                    }
                    if (in.f2.equals("114")) {
                        map.put("101_101_104_109_114", in.f4);
                        if (map.containsKey("101_101_104_109_113") && map.containsKey("101_101_104_109_120") && map.containsKey("101_101_104_109_121")) {
                            double mem_total = Double.parseDouble(map.get("101_101_104_109_113").toString());
                            double mem_available = Double.parseDouble(map.get("101_101_104_109_114").toString());
                            double mem_buffered = Double.parseDouble(map.get("101_101_104_109_120").toString());
                            double mem_cached = Double.parseDouble(map.get("101_101_104_109_121").toString());
                            String result = (1 -(mem_total-mem_available-mem_buffered-mem_cached) / mem_total) * 100 + "";
                            collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                            map.remove("101_101_104_109_113");
                            map.remove("101_101_104_109_114");
                            map.remove("101_101_104_109_120");
                            map.remove("101_101_104_109_121");
                            continue;
                        } else {
                            continue;
                        }
                    }
                    if (in.f2.equals("120")) {
                        map.put("101_101_104_109_120", in.f4);
                        if (map.containsKey("101_101_104_109_114") && map.containsKey("101_101_104_109_113") && map.containsKey("101_101_104_109_121")) {
                            double mem_total = Double.parseDouble(map.get("101_101_104_109_113").toString());
                            double mem_available = Double.parseDouble(map.get("101_101_104_109_114").toString());
                            double mem_buffered = Double.parseDouble(map.get("101_101_104_109_120").toString());
                            double mem_cached = Double.parseDouble(map.get("101_101_104_109_121").toString());
                            String result = (1 -(mem_total-mem_available-mem_buffered-mem_cached) / mem_total) * 100 + "";
                            collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                            map.remove("101_101_104_109_113");
                            map.remove("101_101_104_109_114");
                            map.remove("101_101_104_109_120");
                            map.remove("101_101_104_109_121");
                            continue;
                        } else {
                            continue;
                        }
                    }
                    if (in.f2.equals("121")) {
                        map.put("101_101_104_109_121", in.f4);
                        if (map.containsKey("101_101_104_109_114") && map.containsKey("101_101_104_109_120") && map.containsKey("101_101_104_109_113")) {
                            double mem_total = Double.parseDouble(map.get("101_101_104_109_113").toString());
                            double mem_available = Double.parseDouble(map.get("101_101_104_109_114").toString());
                            double mem_buffered = Double.parseDouble(map.get("101_101_104_109_120").toString());
                            double mem_cached = Double.parseDouble(map.get("101_101_104_109_121").toString());
                            String result = (1 -(mem_total-mem_available-mem_buffered-mem_cached) / mem_total) * 100 + "";
                            collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                            map.remove("101_101_104_109_113");
                            map.remove("101_101_104_109_114");
                            map.remove("101_101_104_109_120");
                            map.remove("101_101_104_109_121");
                            continue;
                        } else {
                            continue;
                        }
                    }

                }


            //内存总数、文件buffer内存、用于缓存的内存、可获取内存数、空闲内存和收包字节总数换成MB单位
            if ("101_101_104_101".equals(in.f1) || "101_101_104_103".equals(in.f1) || "101_101_104_104".equals(in.f1) || "101_101_104_105".equals(in.f1) || "101_101_104_106".equals(in.f1)) {
                String result = Double.parseDouble(in.f4) / 1024 + "";
                collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, in.f2, in.f3, result));
                continue;
            }

            //内存已用百分比
            if ("101_101_104_109".equals(in.f1)) {
                if (in.f2.equals("111")) {
                    map.put("101_101_104_109_111", in.f4);
                    if (map.containsKey("101_101_104_109_112") && map.containsKey("101_101_104_109_118") && map.containsKey("101_101_104_109_119")) {
                        double mem_total = Double.parseDouble(map.get("101_101_104_109_111").toString());
                        double mem_available = Double.parseDouble(map.get("101_101_104_109_112").toString());
                        double mem_buffered = Double.parseDouble(map.get("101_101_104_109_118").toString());
                        double mem_cached = Double.parseDouble(map.get("101_101_104_109_119").toString());
                        String result = ((mem_total-mem_available-mem_buffered-mem_cached) / mem_total) * 100 + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        map.remove("101_101_104_109_111");
                        map.remove("101_101_104_109_112");
                        map.remove("101_101_104_109_118");
                        map.remove("101_101_104_109_119");
                        continue;
                    } else {
                        continue;
                    }
                }
                if (in.f2.equals("112")) {
                    map.put("101_101_104_109_112", in.f4);
                    if (map.containsKey("101_101_104_109_111") && map.containsKey("101_101_104_109_118") && map.containsKey("101_101_104_109_119")) {
                        double mem_total = Double.parseDouble(map.get("101_101_104_109_111").toString());
                        double mem_available = Double.parseDouble(map.get("101_101_104_109_112").toString());
                        double mem_buffered = Double.parseDouble(map.get("101_101_104_109_118").toString());
                        double mem_cached = Double.parseDouble(map.get("101_101_104_109_119").toString());
                        String result = ((mem_total-mem_available-mem_buffered-mem_cached) / mem_total) * 100 + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        map.remove("101_101_104_109_111");
                        map.remove("101_101_104_109_112");
                        map.remove("101_101_104_109_118");
                        map.remove("101_101_104_109_119");
                        continue;
                    } else {
                        continue;
                    }
                }
                if (in.f2.equals("118")) {
                    map.put("101_101_104_109_118", in.f4);
                    if (map.containsKey("101_101_104_109_111") && map.containsKey("101_101_104_109_112") && map.containsKey("101_101_104_109_119")) {
                        double mem_total = Double.parseDouble(map.get("101_101_104_109_111").toString());
                        double mem_available = Double.parseDouble(map.get("101_101_104_109_112").toString());
                        double mem_buffered = Double.parseDouble(map.get("101_101_104_109_118").toString());
                        double mem_cached = Double.parseDouble(map.get("101_101_104_109_119").toString());
                        String result = ((mem_total-mem_available-mem_buffered-mem_cached) / mem_total) * 100 + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        map.remove("101_101_104_109_111");
                        map.remove("101_101_104_109_112");
                        map.remove("101_101_104_109_118");
                        map.remove("101_101_104_109_119");
                        continue;
                    } else {
                        continue;
                    }
                }
                if (in.f2.equals("119")) {
                    map.put("101_101_104_109_119", in.f4);
                    if (map.containsKey("101_101_104_109_111") && map.containsKey("101_101_104_109_118") && map.containsKey("101_101_104_109_112")) {
                        double mem_total = Double.parseDouble(map.get("101_101_104_109_111").toString());
                        double mem_available = Double.parseDouble(map.get("101_101_104_109_112").toString());
                        double mem_buffered = Double.parseDouble(map.get("101_101_104_109_118").toString());
                        double mem_cached = Double.parseDouble(map.get("101_101_104_109_119").toString());
                        String result = ((mem_total-mem_available-mem_buffered-mem_cached) / mem_total) * 100 + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        map.remove("101_101_104_109_111");
                        map.remove("101_101_104_109_112");
                        map.remove("101_101_104_109_118");
                        map.remove("101_101_104_109_119");
                        continue;
                    } else {
                        continue;
                    }
                }
            }

            //已用内存
            if ("101_101_104_102".equals(in.f1)) {
                if (in.f2.equals("115")) {
                    map.put("101_101_104_102_115", in.f4);
                    if (map.containsKey("101_101_104_102_102") && map.containsKey("101_101_104_102_116") && map.containsKey("101_101_104_102_117")) {
                        double mem_total = Double.parseDouble(map.get("101_101_104_102_115").toString());
                        double mem_free = Double.parseDouble(map.get("101_101_104_102_102").toString());
                        double mem_buffered = Double.parseDouble(map.get("101_101_104_102_116").toString());
                        double mem_cached = Double.parseDouble(map.get("101_101_104_102_117").toString());
                        String result = (mem_total - mem_free - mem_buffered - mem_cached) / 1024 + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        map.remove("101_101_104_102_115");
                        map.remove("101_101_104_102_102");
                        map.remove("101_101_104_102_116");
                        map.remove("101_101_104_102_107");
                        continue;
                    } else {
                        continue;
                    }
                }
                if (in.f2.equals("102")) {
                    map.put("101_101_104_102_102", in.f4);
                    if (map.containsKey("101_101_104_102_115") && map.containsKey("101_101_104_102_116") && map.containsKey("101_101_104_102_117")) {
                        double mem_total = Double.parseDouble(map.get("101_101_104_102_115").toString());
                        double mem_free = Double.parseDouble(map.get("101_101_104_102_102").toString());
                        double mem_buffered = Double.parseDouble(map.get("101_101_104_102_116").toString());
                        double mem_cached = Double.parseDouble(map.get("101_101_104_102_117").toString());
                        String result = (mem_total - mem_free - mem_buffered - mem_cached) / 1024 + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        map.remove("101_101_104_102_115");
                        map.remove("101_101_104_102_102");
                        map.remove("101_101_104_102_116");
                        map.remove("101_101_104_102_107");
                        continue;
                    } else {
                        continue;
                    }
                }
                if (in.f2.equals("116")) {
                    map.put("101_101_104_102_116", in.f4);
                    if (map.containsKey("101_101_104_102_115") && map.containsKey("101_101_104_102_102") && map.containsKey("101_101_104_102_117")) {
                        double mem_total = Double.parseDouble(map.get("101_101_104_102_115").toString());
                        double mem_free = Double.parseDouble(map.get("101_101_104_102_102").toString());
                        double mem_buffered = Double.parseDouble(map.get("101_101_104_102_116").toString());
                        double mem_cached = Double.parseDouble(map.get("101_101_104_102_117").toString());
                        String result = (mem_total - mem_free - mem_buffered - mem_cached) / 1024 + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        map.remove("101_101_104_102_115");
                        map.remove("101_101_104_102_102");
                        map.remove("101_101_104_102_116");
                        map.remove("101_101_104_102_107");
                        continue;
                    } else {
                        continue;
                    }
                }
                if (in.f2.equals("117")) {
                    map.put("101_101_104_102_117", in.f4);
                    if (map.containsKey("101_101_104_102_115") && map.containsKey("101_101_104_102_102") && map.containsKey("101_101_104_102_116")) {
                        double mem_total = Double.parseDouble(map.get("101_101_104_102_115").toString());
                        double mem_free = Double.parseDouble(map.get("101_101_104_102_102").toString());
                        double mem_buffered = Double.parseDouble(map.get("101_101_104_102_116").toString());
                        double mem_cached = Double.parseDouble(map.get("101_101_104_102_117").toString());
                        String result = (mem_total - mem_free - mem_buffered - mem_cached) / 1024 + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        map.remove("101_101_104_102_115");
                        map.remove("101_101_104_102_102");
                        map.remove("101_101_104_102_116");
                        map.remove("101_101_104_102_107");
                        continue;
                    } else {
                        continue;
                    }
                }
            }


        /*
        主机swap空间
         */
            //Linux的Swap使用空间占比
            if (getMessage_Two(collector, map, in, "101_101_105_104", "105", "106")) continue;
            //Swap空间总量、Swap已使用空间单位换成MB
            if ("101_101_105_101".equals(in.f1) || "101_101_105_102".equals(in.f1)) {
                String result = Double.parseDouble(in.f4) / 1024 + "";
                collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, in.f2, in.f3, result));
                continue;
            }
            //Swap空闲空间
            if ("101_101_105_103".equals(in.f1)) {
                if (in.f2.equals("103")) {
                    map.put("101_101_105_103_103", in.f4);
                    if (map.containsKey("101_101_105_103_104")) {
                        double swap_total = Double.parseDouble(map.get("101_101_105_103_103").toString());
                        double swap_used = Double.parseDouble(map.get("101_101_105_103_104").toString());
                        String result = swap_total - swap_used + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        map.remove("101_101_105_103_103");
                        map.remove("101_101_105_103_104");
                        continue;
                    } else {
                        continue;
                    }
                }
                if (in.f2.equals("104")) {
                    map.put("101_101_105_103_104", in.f4);
                    if (map.containsKey("101_101_105_103_103")) {
                        double swap_total = Double.parseDouble(map.get("101_101_105_103_103").toString());
                        double swap_used = Double.parseDouble(map.get("101_101_105_103_104").toString());
                        String result = swap_total - swap_used + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        map.remove("101_101_105_103_103");
                        map.remove("101_101_105_103_104");
                        continue;
                    } else {
                        continue;
                    }
                }
            }

            /*
            主机磁盘
             */
            if ("101_101_107_104".equals(in.f1) || "101_101_107_105".equals(in.f1) || "101_101_107_106".equals(in.f1)) {
                String result = Double.parseDouble(in.f4) / 1048576 + "";
                collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, in.f2, in.f3, result));
                continue;
            }


            //路由器(华三 H3C MSR3610)的接受错包速率
            if (in.f1.equals("102_101_101_102")) {
                if (StreamToFlink.totalMap.size() < 10) {
                    StreamToFlink.totalMap.put("102_101_101_102" + "_" + in.f2, in.f4);
                    continue;
                } else if (in.f2.equals("102")) {
                    map.put("102_101_101_102_102", in.f4);
                    if (map.containsKey("102_101_101_102_103") && map.containsKey("102_101_101_102_104") && map.containsKey("102_101_101_102_105") && StreamToFlink.totalMap.size() == 10) {
                        double iflnErrors_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_102").toString());
                        double ifHClnUcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_103").toString());
                        double ifHCInMulticastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_104").toString());
                        double ifHCInBroadcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_105").toString());
                        double iflnErrors_t2 = Double.parseDouble(map.get("102_101_101_102_102").toString());
                        double ifHClnUcastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_103").toString());
                        double ifHCInMulticastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_104").toString());
                        double ifHCInBroadcastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_105").toString());
                        String result = (iflnErrors_t1 - iflnErrors_t2) / (iflnErrors_t1 - iflnErrors_t2 + ifHClnUcastPkts_t1 - ifHClnUcastPkts_t2 + ifHCInMulticastPkts_t1 - ifHCInMulticastPkts_t2 + ifHCInBroadcastPkts_t1 - ifHCInBroadcastPkts_t2) + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        StreamToFlink.totalMap.put("102_101_101_102_102", iflnErrors_t2);
                        StreamToFlink.totalMap.put("102_101_101_102_103", ifHClnUcastPkts_t2);
                        StreamToFlink.totalMap.put("102_101_101_102_104", ifHCInMulticastPkts_t2);
                        StreamToFlink.totalMap.put("102_101_101_102_105", ifHCInBroadcastPkts_t2);
                        map.remove("102_101_101_102_102");
                        map.remove("102_101_101_102_103");
                        map.remove("102_101_101_102_104");
                        map.remove("102_101_101_102_105");
                        continue;
                    } else {
                        continue;
                    }
                } else if (in.f2.equals("103")) {
                    map.put("102_101_101_102_103", in.f4);
                    if (map.containsKey("102_101_101_102_102") && map.containsKey("102_101_101_102_104") && map.containsKey("102_101_101_102_105") && StreamToFlink.totalMap.size() == 10) {
                        double iflnErrors_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_102").toString());
                        double ifHClnUcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_103").toString());
                        double ifHCInMulticastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_104").toString());
                        double ifHCInBroadcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_105").toString());
                        double iflnErrors_t2 = Double.parseDouble(map.get("102_101_101_102_102").toString());
                        double ifHClnUcastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_103").toString());
                        double ifHCInMulticastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_104").toString());
                        double ifHCInBroadcastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_105").toString());
                        String result = (iflnErrors_t1 - iflnErrors_t2) / (iflnErrors_t1 - iflnErrors_t2 + ifHClnUcastPkts_t1 - ifHClnUcastPkts_t2 + ifHCInMulticastPkts_t1 - ifHCInMulticastPkts_t2 + ifHCInBroadcastPkts_t1 - ifHCInBroadcastPkts_t2) + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        StreamToFlink.totalMap.put("102_101_101_102_102", iflnErrors_t2);
                        StreamToFlink.totalMap.put("102_101_101_102_103", ifHClnUcastPkts_t2);
                        StreamToFlink.totalMap.put("102_101_101_102_104", ifHCInMulticastPkts_t2);
                        StreamToFlink.totalMap.put("102_101_101_102_105", ifHCInBroadcastPkts_t2);
                        map.remove("102_101_101_102_102");
                        map.remove("102_101_101_102_103");
                        map.remove("102_101_101_102_104");
                        map.remove("102_101_101_102_105");
                        continue;
                    } else {
                        continue;
                    }
                } else if (in.f2.equals("104")) {
                    map.put("102_101_101_102_104", in.f4);
                    if (map.containsKey("102_101_101_102_102") && map.containsKey("102_101_101_102_103") && map.containsKey("102_101_101_102_105") && StreamToFlink.totalMap.size() == 10) {
                        double iflnErrors_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_102").toString());
                        double ifHClnUcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_103").toString());
                        double ifHCInMulticastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_104").toString());
                        double ifHCInBroadcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_105").toString());
                        double iflnErrors_t2 = Double.parseDouble(map.get("102_101_101_102_102").toString());
                        double ifHClnUcastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_103").toString());
                        double ifHCInMulticastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_104").toString());
                        double ifHCInBroadcastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_105").toString());
                        String result = (iflnErrors_t1 - iflnErrors_t2) / (iflnErrors_t1 - iflnErrors_t2 + ifHClnUcastPkts_t1 - ifHClnUcastPkts_t2 + ifHCInMulticastPkts_t1 - ifHCInMulticastPkts_t2 + ifHCInBroadcastPkts_t1 - ifHCInBroadcastPkts_t2) + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        StreamToFlink.totalMap.put("102_101_101_102_102", iflnErrors_t2);
                        StreamToFlink.totalMap.put("102_101_101_102_103", ifHClnUcastPkts_t2);
                        StreamToFlink.totalMap.put("102_101_101_102_104", ifHCInMulticastPkts_t2);
                        StreamToFlink.totalMap.put("102_101_101_102_105", ifHCInBroadcastPkts_t2);
                        map.remove("102_101_101_102_102");
                        map.remove("102_101_101_102_103");
                        map.remove("102_101_101_102_104");
                        map.remove("102_101_101_102_105");
                        continue;
                    } else {
                        continue;
                    }
                } else if (in.f2.equals("105")) {
                    map.put("102_101_101_102_105", in.f4);
                    if (map.containsKey("102_101_101_102_102") && map.containsKey("102_101_101_102_103") && map.containsKey("102_101_101_102_104") && StreamToFlink.totalMap.size() == 10) {
                        double iflnErrors_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_102").toString());
                        double ifHClnUcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_103").toString());
                        double ifHCInMulticastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_104").toString());
                        double ifHCInBroadcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_105").toString());
                        double iflnErrors_t2 = Double.parseDouble(map.get("102_101_101_102_102").toString());
                        double ifHClnUcastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_103").toString());
                        double ifHCInMulticastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_104").toString());
                        double ifHCInBroadcastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_105").toString());
                        String result = (iflnErrors_t1 - iflnErrors_t2) / (iflnErrors_t1 - iflnErrors_t2 + ifHClnUcastPkts_t1 - ifHClnUcastPkts_t2 + ifHCInMulticastPkts_t1 - ifHCInMulticastPkts_t2 + ifHCInBroadcastPkts_t1 - ifHCInBroadcastPkts_t2) + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        StreamToFlink.totalMap.put("102_101_101_102_102", iflnErrors_t2);
                        StreamToFlink.totalMap.put("102_101_101_102_103", ifHClnUcastPkts_t2);
                        StreamToFlink.totalMap.put("102_101_101_102_104", ifHCInMulticastPkts_t2);
                        StreamToFlink.totalMap.put("102_101_101_102_105", ifHCInBroadcastPkts_t2);
                        map.remove("102_101_101_102_102");
                        map.remove("102_101_101_102_103");
                        map.remove("102_101_101_102_104");
                        map.remove("102_101_101_102_105");
                        continue;
                    } else {
                        continue;
                    }
                }
            }

            //路由器(华三 H3C MSR3610)的发送错包速率
            if (in.f1.equals("102_101_101_103")) {
                if (StreamToFlink.totalMap.size() < 10) {
                    StreamToFlink.totalMap.put("102_101_101_103" + "_" + in.f2, in.f4);
                    continue;
                } else if (in.f2.equals("106")) {
                    map.put("102_101_101_103_106", in.f4);
                    if (map.containsKey("102_101_101_103_107") && map.containsKey("102_101_101_103_108") && map.containsKey("102_101_101_103_109") && StreamToFlink.totalMap.size() == 10) {
                        double ifOutErrors_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_106").toString());
                        double ifHCOutUcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_107").toString());
                        double ifHCOutMulticastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_108").toString());
                        double ifHCOutBroadcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_109").toString());
                        double ifOutErrors_t2 = Double.parseDouble(map.get("102_101_101_103_106").toString());
                        double ifHCOutUcastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_107").toString());
                        double ifHCOutMulticastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_108").toString());
                        double ifHCOutBroadcastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_109").toString());
                        String result = (ifOutErrors_t1 - ifOutErrors_t2) / (ifOutErrors_t1 - ifOutErrors_t2 + ifHCOutUcastPkts_t1 - ifHCOutUcastPkts_t2 + ifHCOutMulticastPkts_t1 - ifHCOutMulticastPkts_t2 + ifHCOutBroadcastPkts_t1 - ifHCOutBroadcastPkts_t2) + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        StreamToFlink.totalMap.put("102_101_101_103_106", ifOutErrors_t2);
                        StreamToFlink.totalMap.put("102_101_101_103_107", ifHCOutUcastPkts_t2);
                        StreamToFlink.totalMap.put("102_101_101_103_108", ifHCOutMulticastPkts_t2);
                        StreamToFlink.totalMap.put("102_101_101_103_109", ifHCOutBroadcastPkts_t2);
                        map.remove("102_101_101_103_106");
                        map.remove("102_101_101_103_107");
                        map.remove("102_101_101_103_108");
                        map.remove("102_101_101_103_109");
                        continue;
                    } else {
                        continue;
                    }
                } else if (in.f2.equals("107")) {
                    map.put("102_101_101_103_107", in.f4);
                    if (map.containsKey("102_101_101_103_106") && map.containsKey("102_101_101_103_108") && map.containsKey("102_101_101_103_109") && StreamToFlink.totalMap.size() == 10) {
                        double ifOutErrors_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_106").toString());
                        double ifHCOutUcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_107").toString());
                        double ifHCOutMulticastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_108").toString());
                        double ifHCOutBroadcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_109").toString());
                        double ifOutErrors_t2 = Double.parseDouble(map.get("102_101_101_103_106").toString());
                        double ifHCOutUcastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_107").toString());
                        double ifHCOutMulticastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_108").toString());
                        double ifHCOutBroadcastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_109").toString());
                        String result = (ifOutErrors_t1 - ifOutErrors_t2) / (ifOutErrors_t1 - ifOutErrors_t2 + ifHCOutUcastPkts_t1 - ifHCOutUcastPkts_t2 + ifHCOutMulticastPkts_t1 - ifHCOutMulticastPkts_t2 + ifHCOutBroadcastPkts_t1 - ifHCOutBroadcastPkts_t2) + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        StreamToFlink.totalMap.put("102_101_101_103_106", ifOutErrors_t2);
                        StreamToFlink.totalMap.put("102_101_101_103_107", ifHCOutUcastPkts_t2);
                        StreamToFlink.totalMap.put("102_101_101_103_108", ifHCOutMulticastPkts_t2);
                        StreamToFlink.totalMap.put("102_101_101_103_109", ifHCOutBroadcastPkts_t2);
                        map.remove("102_101_101_103_106");
                        map.remove("102_101_101_103_107");
                        map.remove("102_101_101_103_108");
                        map.remove("102_101_101_103_109");
                        continue;
                    } else {
                        continue;
                    }
                } else if (in.f2.equals("108")) {
                    map.put("102_101_101_103_108", in.f4);
                    if (map.containsKey("102_101_101_103_107") && map.containsKey("102_101_101_103_106") && map.containsKey("102_101_101_103_109") && StreamToFlink.totalMap.size() == 10) {
                        double ifOutErrors_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_106").toString());
                        double ifHCOutUcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_107").toString());
                        double ifHCOutMulticastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_108").toString());
                        double ifHCOutBroadcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_109").toString());
                        double ifOutErrors_t2 = Double.parseDouble(map.get("102_101_101_103_106").toString());
                        double ifHCOutUcastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_107").toString());
                        double ifHCOutMulticastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_108").toString());
                        double ifHCOutBroadcastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_109").toString());
                        String result = (ifOutErrors_t1 - ifOutErrors_t2) / (ifOutErrors_t1 - ifOutErrors_t2 + ifHCOutUcastPkts_t1 - ifHCOutUcastPkts_t2 + ifHCOutMulticastPkts_t1 - ifHCOutMulticastPkts_t2 + ifHCOutBroadcastPkts_t1 - ifHCOutBroadcastPkts_t2) + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        StreamToFlink.totalMap.put("102_101_101_103_106", ifOutErrors_t2);
                        StreamToFlink.totalMap.put("102_101_101_103_107", ifHCOutUcastPkts_t2);
                        StreamToFlink.totalMap.put("102_101_101_103_108", ifHCOutMulticastPkts_t2);
                        StreamToFlink.totalMap.put("102_101_101_103_109", ifHCOutBroadcastPkts_t2);
                        map.remove("102_101_101_103_106");
                        map.remove("102_101_101_103_107");
                        map.remove("102_101_101_103_108");
                        map.remove("102_101_101_103_109");
                        continue;
                    } else {
                        continue;
                    }
                } else if (in.f2.equals("109")) {
                    map.put("102_101_101_103_109", in.f4);
                    if (map.containsKey("102_101_101_103_107") && map.containsKey("102_101_101_103_108") && map.containsKey("102_101_101_103_106") && StreamToFlink.totalMap.size() == 10) {
                        double ifOutErrors_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_106").toString());
                        double ifHCOutUcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_107").toString());
                        double ifHCOutMulticastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_108").toString());
                        double ifHCOutBroadcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_109").toString());
                        double ifOutErrors_t2 = Double.parseDouble(map.get("102_101_101_103_106").toString());
                        double ifHCOutUcastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_107").toString());
                        double ifHCOutMulticastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_108").toString());
                        double ifHCOutBroadcastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_109").toString());
                        String result = (ifOutErrors_t1 - ifOutErrors_t2) / (ifOutErrors_t1 - ifOutErrors_t2 + ifHCOutUcastPkts_t1 - ifHCOutUcastPkts_t2 + ifHCOutMulticastPkts_t1 - ifHCOutMulticastPkts_t2 + ifHCOutBroadcastPkts_t1 - ifHCOutBroadcastPkts_t2) + "";
                        collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                        StreamToFlink.totalMap.put("102_101_101_103_106", ifOutErrors_t2);
                        StreamToFlink.totalMap.put("102_101_101_103_107", ifHCOutUcastPkts_t2);
                        StreamToFlink.totalMap.put("102_101_101_103_108", ifHCOutMulticastPkts_t2);
                        StreamToFlink.totalMap.put("102_101_101_103_109", ifHCOutBroadcastPkts_t2);
                        map.remove("102_101_101_103_106");
                        map.remove("102_101_101_103_107");
                        map.remove("102_101_101_103_108");
                        map.remove("102_101_101_103_109");
                        continue;
                    } else {
                        continue;
                    }
                }
            }

            //路由器(华三 H3C MSR3610)的接收包速率
            if (in.f1.equals("102_101_101_104")) {
                if (StreamToFlink.totalMap.size() < 10) {
                    StreamToFlink.totalMap.put("102_101_101_104" + "_" + in.f2, in.f4);
                    continue;
                } else {
                    map.put("102_101_101_104_110", in.f4);
                    double ifHCInOctets_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_104_110").toString());
                    double ifHCInOctets_t2 = Double.parseDouble(map.get("102_101_101_104_110").toString());
                    String result = 8 * (ifHCInOctets_t1 - ifHCInOctets_t2) / 5000 + "";
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                    StreamToFlink.totalMap.put("102_101_101_104_110", ifHCInOctets_t2);
                    map.remove("102_101_101_104_110");
                    continue;
                }
            }

            //路由器(华三 H3C MSR3610)的发送包速率
            if (in.f1.equals("102_101_101_105")) {
                if (StreamToFlink.totalMap.size() < 10) {
                    StreamToFlink.totalMap.put("102_101_101_105" + "_" + in.f2, in.f4);
                    continue;
                } else {
                    map.put("102_101_101_105_111", in.f3);
                    double ifHCOutOctets_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_105_111").toString());
                    double ifHCOutOctets_t2 = Double.parseDouble(map.get("102_101_101_105_111").toString());
                    String result = 8 * (ifHCOutOctets_t1 - ifHCOutOctets_t2) / 5000 + "";
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                    StreamToFlink.totalMap.put("102_101_101_105_111", ifHCOutOctets_t2);
                    map.remove("102_101_101_105_111");
                    continue;
                }
            }


            //铱迅 NGFW-5731-W防火墙的内存使用率
            if (getMessage_Two(collector, map, in, "102_104_101_101", "101", "102")) continue;
            //铱迅 NGFW-5731-W防火墙的内存使用率
            if (getMessage_Two(collector, map, in, "102_104_102_103", "104", "105")) continue;


            collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, in.f2, in.f3, in.f4));
        }

      }
    }

    private boolean getMessage_Three(Collector<Tuple6<String, String, String, String, String, String>> collector, Map map, Tuple5<Tuple2<String, String>, String, String, String, String > in, String s, String s2, String s3, String s4) {
        if (in.f1.equals(s)) {
            int i = in.f2.lastIndexOf(".");
            String number = in.f2.substring(i);

            s2 += number;
            s3 += number;
            s4 += number;
            if (in.f2.equals(s2)) {
                map.put(in.f1+s2, in.f4);
                if (map.containsKey(in.f1+s3) && map.containsKey(in.f1+s4)) {
                    double rPackageSingle = Double.parseDouble(map.get(in.f1+s2).toString());
                    double rPackageDouble = Double.parseDouble(map.get(in.f1+s3).toString());
                    double rLostPackageNum = Double.parseDouble(map.get(in.f1+s4).toString());
                    String result = rLostPackageNum / (rPackageSingle + rPackageDouble) +"";
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1+number, "000", in.f3, result));
                    map.remove(in.f1+s2);
                    map.remove(in.f1+s3);
                    map.remove(in.f1+s4);
                    return true;
                } else {
                    return true;
                }
            }
            if (in.f2.equals(s3)) {
                map.put(in.f1+s3, in.f4);
                if (map.containsKey(in.f1+s2) && map.containsKey(in.f1+s4)) {
                    double rPackageSingle = Double.parseDouble(map.get(in.f1+s2).toString());
                    double rPackageDouble = Double.parseDouble(map.get(in.f1+s3).toString());
                    double rLostPackageNum = Double.parseDouble(map.get(in.f1+s4).toString());
                    String result = rLostPackageNum / (rPackageSingle + rPackageDouble) +"";
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1+number, "000", in.f3, result));
                    map.remove(in.f1+s2);
                    map.remove(in.f1+s3);
                    map.remove(in.f1+s4);
                    return true;
                } else {
                    return true;
                }
            }
            if (in.f2.equals(s4)) {
                map.put(in.f1+s4, in.f4);
                if (map.containsKey(in.f1+s2) && map.containsKey(in.f1+s3)) {
                    double rPackageSingle = Double.parseDouble(map.get(in.f1+s2).toString());
                    double rPackageDouble = Double.parseDouble(map.get(in.f1+s3).toString());
                    double rLostPackageNum = Double.parseDouble(map.get(in.f1+s4).toString());
                    String result = rLostPackageNum / (rPackageSingle + rPackageDouble) +"";
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1+number, "000", in.f3, result));
                    map.remove(in.f1+s2);
                    map.remove(in.f1+s3);
                    map.remove(in.f1+s4);
                    return true;
                } else {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean getMessage_Two(Collector<Tuple6<String, String, String, String, String, String>> collector, Map map, Tuple5<Tuple2<String, String>, String, String, String, String> in, String s, String s2, String s3) {
        if (in.f1.contains(s)) {
            if (in.f2.equals(s2)) {
                map.put(in.f1+s2,in.f4);
                if (map.containsKey(in.f1+s3)) {
                    double memory_All = Double.parseDouble(map.get(in.f1+s2).toString());
                    double memory_available = Double.parseDouble(map.get(in.f1+s3).toString());
                    String result = (memory_available / memory_All)*100 +"";
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                    map.remove(in.f1+s2);
                    map.remove(in.f1+s3);
                    return true;
                } else {
                    return true;
                }
            }
            if (in.f2.equals(s3)) {
                map.put(in.f1+s3, in.f4);
                if (map.containsKey(in.f1+s2)) {
                    double memory_All = Double.parseDouble(map.get(in.f1+s2).toString());
                    double memory_available = Double.parseDouble(map.get(in.f1+s3).toString());
                    String result = (memory_available / memory_All)*100 +"";
                    collector.collect(Tuple6.of(in.f0.f0, in.f0.f1, in.f1, "000", in.f3, result));
                    map.remove(in.f1+s2);
                    map.remove(in.f1+s3);
                    return true;
                } else {
                    return true;
                }
            }
        }
        return false;
    }
}
