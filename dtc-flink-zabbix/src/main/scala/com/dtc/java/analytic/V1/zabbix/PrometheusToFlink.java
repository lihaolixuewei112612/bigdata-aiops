package com.dtc.java.analytic.V1.zabbix;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dtc.java.analytic.V1.common.utils.CountUtils;
import com.dtc.java.analytic.V1.common.utils.ExecutionEnvUtil;
import com.dtc.java.analytic.V1.common.utils.KafkaConfigUtil;
import com.dtc.java.analytic.V1.common.watermarks.DTCPeriodicWatermak;
import com.dtc.java.analytic.V1.sink.opentsdb.PSinkToOpentsdb;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dtc.java.analytic.V1.hbase.hbase.constant.HBaseConstant.*;

/**
 * Created on 2019-08-15
 *
 * @author :hao.li
 */
public class PrometheusToFlink {
    private static TableName HBASE_TABLE_NAME = TableName.valueOf("dtc_stream");
    //列族
    private static final String INFO_STREAM = "info_stream";

    private static final Logger logger = LoggerFactory.getLogger(PrometheusToFlink.class);

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<String> streamSource = KafkaConfigUtil.buildSource(env);
        SingleOutputStreamOperator<String> metricEventSingleOutputStreamOperator = streamSource.assignTimestampsAndWatermarks(new DTCPeriodicWatermak());
        SingleOutputStreamOperator<Tuple2<String, Tuple6<String, String, String, String, Long, String>>> map = metricEventSingleOutputStreamOperator
                .map(new DTCZabbixMapFunction());
        SplitStream<Tuple2<String, Tuple6<String, String, String, String, Long, String>>> splitStream
                = map.split((OutputSelector<Tuple2<String, Tuple6<String, String, String, String, Long, String>>>) event -> {
            List<String> output = new ArrayList<>();
            String systemName = event.f1.f3;
            if ("字符串类型".equals(systemName)) {
                output.add("Hbase_Sink");
            } else {
                output.add("OPentsdb_Sink");
            }
            return output;
        });

        DataStream<Tuple2<String, Tuple6<String, String, String, String, Long, String>>> hbase_sink = splitStream.select("Hbase_Sink");
        hbase_sink.map(new MapFunction<Tuple2<String, Tuple6<String, String, String, String, Long, String>>, Object>() {
            @Override
            public Object map(Tuple2<String, Tuple6<String, String, String, String, Long, String>> string) throws Exception {
                writeEventToHbase(string, parameterTool);
                return string;
            }
        }).print();
        DataStream<Tuple2<String, Tuple6<String, String, String, String, Long, String>>> oPentsdb_sink = splitStream.select("OPentsdb_Sink");
        WindowedStream<Tuple2<String, Tuple6<String, String, String, String, Long, String>>, Tuple, TimeWindow> tuple2TupleTimeWindowWindowedStream = oPentsdb_sink
                .keyBy(0)
                .timeWindow(Time.milliseconds(5));
        SingleOutputStreamOperator<Tuple7<String, String, String, String, String, Long, String>> apply = tuple2TupleTimeWindowWindowedStream
                .apply(new DtcApplyWindowFunction());
        String opentsdb_url = parameterTool.get("dtc.opentsdb.url", "http://10.3.7.234:4399");
        apply.addSink(new PSinkToOpentsdb(opentsdb_url)).name("Opentsdb-Sink");
        //mysql sink
//      process.addSink(new MysqlSink(properties));
        env.execute("Start zabbix data.");
    }
    private static void writeEventToHbase(Tuple2<String, Tuple6<String, String, String, String, Long, String>> str, ParameterTool parameterTool) throws IOException {
       //(10.3.7.233,(node02,.1.3.6.1.4.1.2021.10.1.3.2,system_load5,system_load5,1577173636,0.27))
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HBASE_ZOOKEEPER_QUORUM, parameterTool.get(HBASE_ZOOKEEPER_QUORUM));
        configuration.set(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, parameterTool.get(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT));
        configuration.set(HBASE_RPC_TIMEOUT, parameterTool.get(HBASE_RPC_TIMEOUT));
        configuration.set(HBASE_CLIENT_OPERATION_TIMEOUT, parameterTool.get(HBASE_CLIENT_OPERATION_TIMEOUT));
        configuration.set(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, parameterTool.get(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD));
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        Connection connect = ConnectionFactory.createConnection(configuration);
        Admin admin = connect.getAdmin();
        if (!admin.tableExists(HBASE_TABLE_NAME)) { //检查是否有该表，如果没有，创建
            admin.createTable(new HTableDescriptor(HBASE_TABLE_NAME).addFamily(new HColumnDescriptor(INFO_STREAM)));
        }
        Table table = connect.getTable(HBASE_TABLE_NAME);
        Put put = new Put(Bytes.toBytes(str.f0+":"+str.f1.f1+":"+str.f1.f4));
        put.addColumn(Bytes.toBytes(INFO_STREAM), Bytes.toBytes("value"), Bytes.toBytes(str.f1.f5));
        table.put(put);
        table.close();
        connect.close();
    }
}
class DTCZabbixMapFunction implements MapFunction<String, Tuple2<String, Tuple6<String, String, String, String, Long, String>>> {

    @Override
    public Tuple2<String, Tuple6<String, String, String, String, Long, String>> map(String event) {
        CountUtils.incrementEventReceivedCount();
        Tuple2<String, Tuple6<String, String, String, String, Long, String>> message;
        JSONObject object = JSON.parseObject(event);
        String ip = object.getString("ip");
        String host = object.getString("host");
        String oid = object.getString("oid");
        String name = object.getString("name");
        String english = object.getString("english");
        long clock = object.getLong("clock");
        String value = object.getString("value");
        boolean b = strIsNum(value);
        if(b) {
            message = Tuple2.of(ip, Tuple6.of(host, oid, name, english, clock, value));
        }else {
            message = Tuple2.of(ip, Tuple6.of(host, oid, name, "字符串类型", clock, value));
        }
        return message;
    }

    private boolean strIsNum(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}

//public class DTCZabbixMapFunction implements MapFunction<String, Tuple2<String, Tuple6<String, String, String, String, Long, Double>>> {
//
//    @Override
//    public Tuple2<String, Tuple6<String, String, String, String, Long, Double>> map(String event) {
//        CountUtils.incrementEventReceivedCount();
//        Tuple2<String, Tuple6<String, String, String, String, Long, Double>> message;
//        JSONObject object = JSON.parseObject(event);
//        String ip = object.getString("ip");
//        String host = object.getString("host");
//        String oid = object.getString("oid");
//        String name = object.getString("name");
//        String english = object.getString("english");
//        long clock = object.getLong("clock");
//
//        double value = object.getDouble("value");
//        message = Tuple2.of(ip, Tuple6.of(host, oid, name, english, clock, value));
//        return message;
//    }
//}

//class DtcMapFunction implements MapFunction<String, Tuple5<String, String, String, Long, Double>> {
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(DTCPeriodicWatermak.class);
//
//    @Override
//    public Tuple5<String, String, String, Long, Double> map(String s) {
//        CountUtils.incrementEventReceivedCount();
//        Tuple5<String, String, String, Long, Double> message = null;
//        ObjectMapper objectMapper = new ObjectMapper();
//        JsonNode json = null;
//        try {
//            json = objectMapper.readTree(s);
//        } catch (IOException e) {
//            LOGGER.error("Make mistake when parse the data,and the reason is {}.", e);
//            CountUtils.incrementEventReceivedCount_Failuer();
//            return Tuple5.of("null", "null", "null", null, null);
//        }
//        String name1 = json.get("name").textValue();
//        if (!(name1.contains("_"))) {
//            name1 = name1 + "_" + 0000;
//        }
//        String[] name = name1.split("_", 2);
//        String system_name = name[0].trim();
//        String ZB_name = name[1].trim();
////-----------------------------------------------------------------------------
////        Long time_all = Untils.getTime(json.get("timestamp").textValue());
////        Double value_all = json.get("value").asDouble();
////        JsonNode jsonNode_all = null;
////        try {
////            jsonNode_all = objectMapper.readTree(json.get("labels").toString());
////        } catch (IOException e) {
////            CountUtils.incrementEventReceivedCount_Failuer();
////            LOGGER.error("Make mistake when parse the lebles of data,and the reason is {}.", e);
////            return Tuple5.of("null", "null", "null", null, null);
////        }
////        String Host_all = jsonNode_all.get("instance").textValue();
////        message = Tuple5.of(system_name, Host_all, ZB_name, time_all, value_all);
////-----------------------------------------------------------------------------
//////        //Linux数据处理
//        if (system_name.contains("node")) {
//            Long time = Untils.getTime(json.get("timestamp").textValue());
//            Double value = json.get("value").asDouble();
//            JsonNode jsonNode = null;
//            try {
//                jsonNode = objectMapper.readTree(json.get("labels").toString());
//            } catch (IOException e) {
//                CountUtils.incrementEventErrorCount("linux");
//                LOGGER.error("Make mistake when parse the lebles of data,and the reason is {}.", e);
//                return Tuple5.of("null", "null", "null", null, null);
//
//            }
//            String Host = jsonNode.get("instance").textValue();
//            message = Tuple5.of(system_name, Host, ZB_name, time, value);
//            CountUtils.incrementEventRightCount("linux");
//        }
//        //mysql数据处理
//        else if (system_name.contains("mysql")) {
//            Long time = Untils.getTime(json.get("timestamp").textValue());
//            Double value = json.get("value").asDouble();
//            JsonNode jsonNode = null;
//            try {
//                jsonNode = objectMapper.readTree(json.get("labels").toString());
//            } catch (IOException e) {
//                LOGGER.error("Make mistake when parse the lebles of data,and the reason is {}.", e);
//                CountUtils.incrementEventErrorCount("mysql");
//                return null;
//            }
//            String Host = jsonNode.get("instance").textValue();
//            message = Tuple5.of(system_name, Host, ZB_name, time, value);
//            CountUtils.incrementEventRightCount("mysql");
//        }
//        //oracle数据处理
//        else if (system_name.contains("oracledb")) {
//            Long time = Untils.getTime(json.get("timestamp").textValue());
//            Double value = json.get("value").asDouble();
//            JsonNode jsonNode = null;
//            try {
//                jsonNode = objectMapper.readTree(json.get("labels").toString());
//            } catch (IOException e) {
//                LOGGER.error("Make mistake when parse the lebles of data,and the reason is {}.", e);
//                CountUtils.incrementEventErrorCount("oracle");
//                return Tuple5.of("null", "null", "null", null, null);
//            }
//            String Host = jsonNode.get("instance").textValue();
//            message = Tuple5.of(system_name, Host, ZB_name, time, value);
//            CountUtils.incrementEventRightCount("oracle");
//        }
//        //windows数据处理
//        else if (system_name.contains("windows")) {
//            Long time = Untils.getTime(json.get("timestamp").textValue());
//            Double value = json.get("value").asDouble();
//            JsonNode jsonNode = null;
//            try {
//                jsonNode = objectMapper.readTree(json.get("labels").toString());
//            } catch (IOException e) {
//                LOGGER.error("Make mistake when parse the lebles of data,and the reason is {}.", e);
//                CountUtils.incrementEventErrorCount("windows");
//                return Tuple5.of("null", "null", "null", null, null);
//            }
//            String Host = jsonNode.get("instance").textValue();
//            message = Tuple5.of(system_name, Host, ZB_name, time, value);
//            CountUtils.incrementEventRightCount("windows");
//        } else {
////            Long time_all = Untils.getTime(json.get("timestamp").textValue());
////            Double value_all = json.get("value").asDouble();
////            JsonNode jsonNode_all = null;
////            try {
////                jsonNode_all = objectMapper.readTree(json.get("labels").toString());
////            } catch (IOException e) {
////                CountUtils.incrementEventReceivedCount_Failuer();
////                LOGGER.error("Make mistake when parse the lebles of data,and the reason is {}.", e);
////                return Tuple5.of("null", "null", "null", null, null);
////            }
////            String Host_all = jsonNode_all.get("instance").textValue();
////            message = Tuple5.of(system_name, Host_all, ZB_name, time_all, value_all);
//            return Tuple5.of("null", "null", "null", null, null);
//        }
//        return message;
//    }
//}
//
//
//class DtcProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Tuple6<String, String, String, String, Long, Double>>, Tuple7<String, String, String, String, String, Long, Double>, Tuple, TimeWindow> {
//
//    @Override
//    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Tuple6<String, String, String, String, Long, Double>>> elements, Collector<Tuple7<String, String, String, String, String, Long, Double>> collector)
//            throws Exception {
//        for (Tuple2<String, Tuple6<String, String, String, String, Long, Double>> in : elements) {
//            System.out.println(in);
//            collector.collect(Tuple7.of(in.f0, in.f1.f0, in.f1.f1, in.f1.f2, in.f1.f3, in.f1.f4, in.f1.f5));
//        }
//
//    }
//}
////class DtcApplyWindowFunction implements WindowFunction<Tuple2<String, Tuple6<String, String, String, String, Long, Double>>, Tuple7<String, String, String, String, String, Long, Double>, Tuple, TimeWindow> {
////
////    @Override
////    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Tuple6<String, String, String, String, Long, Double>>> iterable, Collector<Tuple7<String, String, String, String, String, Long, Double>> collector) throws Exception {
////        Map<String,Map<String,Double>> map = new HashMap<>();
////        for (Tuple2<String, Tuple6<String, String, String, String, Long, Double>> in : iterable) {
////                Map<String,Double> lihao = new HashMap<>();
////                if(in.f1.f1.equals(".1.3.6.1.4.1.2021.10.1.3.1")){
////                    if(map.containsKey(in.f0)&&map.get(in.f0).containsKey(".1.3.6.1.4.1.2021.10.1.3.2")){
////                        double a = in.f1.f5/map.get(in.f0).get(".1.3.6.1.4.1.2021.10.1.3.2");
////                        collector.collect(Tuple7.of(in.f0, in.f1.f0, in.f1.f1, in.f1.f2, in.f1.f3, in.f1.f4, a));
////                        map.remove("10.3.7.232");
////                        continue;
////                    }else{
////                        lihao.put(in.f1.f1,in.f1.f5);
////                        map.put(in.f0,lihao);
////                    }
////                }else if(in.f1.f1.equals(".1.3.6.1.4.1.2021.10.1.3.2")){
////                    if(map.containsKey(in.f0)&&map.get(in.f0).containsKey(".1.3.6.1.4.1.2021.10.1.3.1")){
////                        double a = in.f1.f5/map.get(in.f0).get(".1.3.6.1.4.1.2021.10.1.3.1");
////                        collector.collect(Tuple7.of(in.f0, in.f1.f0, in.f1.f1, in.f1.f2, in.f1.f3, in.f1.f4, a));
////                        map.remove("10.3.7.232");
////                        continue;
////                    }else{
////                        lihao.put(in.f1.f1,in.f1.f5);
////                        map.put(in.f0,lihao);
////                    }
////                }
////        }
////        System.out.println("lihao-----------------------");
////        Tuple2<String, Tuple6<String, String, String, String, Long, Double>> in = iterable.iterator().next();
////        String format = String.format("data:   %s  startTime:  %s    Endtim:  %s", in.toString(), timeWindow.getStart(), timeWindow.getEnd());
////        System.out.println(format);
//////        collector.collect(Tuple7.of(in.f0, in.f1.f0, in.f1.f1, in.f1.f2, in.f1.f3, in.f1.f4, in.f1.f5));
////    }
////}
//

class DtcApplyWindowFunction implements WindowFunction<Tuple2<String, Tuple6<String, String, String, String, Long, String>>, Tuple7<String, String, String, String, String, Long, String>, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Tuple6<String, String, String, String, Long, String>>> iterable, Collector<Tuple7<String, String, String, String, String, Long, String>> collector) throws Exception {
        Map<String, Map<String, Double>> map = new HashMap<>();
        for (Tuple2<String, Tuple6<String, String, String, String, Long, String>> in : iterable) {

            Map<String, Double> lihao = new HashMap<>();
            if (in.f1.f1.equals(".1.3.6.1.4.1.2021.10.1.3.1")) {
                if (map.containsKey(in.f0) && map.get(in.f0).containsKey(".1.3.6.1.4.1.2021.10.1.3.2")) {
                    double a = Double.parseDouble(in.f1.f5) / map.get(in.f0).get(".1.3.6.1.4.1.2021.10.1.3.2");
                    collector.collect(Tuple7.of(in.f0, in.f1.f0, in.f1.f1, in.f1.f2, in.f1.f3, in.f1.f4, String.valueOf(a)));
                    map.remove("10.3.7.232");
                    continue;
                } else {
                    lihao.put(in.f1.f1, Double.parseDouble(in.f1.f5));
                    map.put(in.f0, lihao);
                }
            } else if (in.f1.f1.equals(".1.3.6.1.4.1.2021.10.1.3.2")) {
                if (map.containsKey(in.f0) && map.get(in.f0).containsKey(".1.3.6.1.4.1.2021.10.1.3.1")) {
                    double a = Double.parseDouble(in.f1.f5 )/ map.get(in.f0).get(".1.3.6.1.4.1.2021.10.1.3.1");
                    collector.collect(Tuple7.of(in.f0, in.f1.f0, in.f1.f1, in.f1.f2, in.f1.f3, in.f1.f4, String.valueOf(a)));
                    map.remove("10.3.7.232");
                    continue;
                } else {
                    lihao.put(in.f1.f1, Double.parseDouble(in.f1.f5));
                    map.put(in.f0, lihao);
                }
            }
        }
        System.out.println("lihao-----------------------");
        Tuple2<String, Tuple6<String, String, String, String, Long, String>> in = iterable.iterator().next();
        String format = String.format("data:   %s  startTime:  %s    Endtim:  %s", in.toString(), timeWindow.getStart(), timeWindow.getEnd());
        System.out.println(format);
        collector.collect(Tuple7.of(in.f0, in.f1.f0, in.f1.f1, in.f1.f2, in.f1.f3, in.f1.f4, in.f1.f5));
    }
}
