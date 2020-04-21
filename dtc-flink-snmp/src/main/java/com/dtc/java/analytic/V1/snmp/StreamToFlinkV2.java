//package com.dtc.java.analytic.snmp;
//
//import com.alibaba.fastjson.JSON;
//import com.dtc.java.analytic.V1.process.function.LinuxProcessMapFunction;
//import com.dtc.java.analytic.common.utils.ExecutionEnvUtil;
//import com.dtc.java.analytic.common.utils.KafkaConfigUtil;
//import com.dtc.java.analytic.common.watermarks.DtcPeriodicAssigner;
//import com.dtc.java.analytic.map.function.DpiSwitchMapFunction;
//import com.dtc.java.analytic.map.function.H3cSwitchMapFunction;
//import com.dtc.java.analytic.map.function.LinuxMapFunction;
//import com.dtc.java.analytic.map.function.ZxSwitchMapFunction;
//import com.dtc.java.analytic.process.function.DPIProcessMapFunction;
//import com.dtc.java.analytic.process.function.H3CSwitchProcessMapFunction;
//import com.dtc.java.analytic.process.function.LinuxProcessMapFunction;
//import com.dtc.java.analytic.process.function.ZXSwitchProcessMapFunction;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.functions.RichMapFunction;
//import org.apache.flink.api.common.state.BroadcastState;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.java.tuple.Tuple5;
//import org.apache.flink.api.java.tuple.Tuple6;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.collector.selector.OutputSelector;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.datastream.SplitStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.util.Collector;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;
//
//import static com.dtc.java.analytic.common.constant.HBaseConstant.*;
//
//
///**
// * Created on 2019-08-12
// *
// * @author :ren
// */
//@Slf4j
//public class StreamToFlinkV2 {
//    final static MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
//            "alarm_rules",
//            BasicTypeInfo.STRING_TYPE_INFO,
//            BasicTypeInfo.STRING_TYPE_INFO);
//
//    public static void main(String[] args) throws Exception {
//        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
//        int windowSizeMillis = parameterTool.getInt("dtc.windowSizeMillis", 2000);
//        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
//        DataStreamSource<Map<String, String>> alarmDataStream = env.addSource(new JSC_AllNum()).setParallelism(1);
//        DataStreamSource<String> streamSource = KafkaConfigUtil.buildSource(env);
////        DataStreamSource<String> dataStreamSource = env.socketTextStream("172.20.10.2", 8080, '\n');
//
//        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, String>> mapStream = streamSource.map(new MyMapFunctionV2());
//        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, String>> timeSingleOutputStream
//                = mapStream.filter(event -> !("null".equals(event.f0))).assignTimestampsAndWatermarks(new DtcPeriodicAssigner());
//
//        SplitStream<Tuple6<String, String, String, String, String, String>> splitStream
//                = timeSingleOutputStream.split((OutputSelector<Tuple6<String, String, String, String, String, String>>) event -> {
//            List<String> output = new ArrayList<>();
//            String type = event.f0;
//            if ("101_100".equals(type)) {
//                output.add("Win");
//            } else if ("101_101".equals(type)) {
//                output.add("Linux");
//            } else if ("102_101".equals(type)) {
//                output.add("H3C_Switch");
//            } else if ("102_102".equals(type)) {
//                output.add("HW_Switch");
//            } else if ("102_103".equals(type)) {
//                output.add("ZX_Switch");
//            } else if ("103_102".equals(type)) {
//                output.add("DPI");
//            }
//            return output;
//        });
//        //windows指标数据处理
//        DataStream<Tuple6<String, String, String, String, String, String>> win = splitStream.select("Win");
//
//        //linux指标数据处理
//        DataStream<Tuple6<String, String, String, String, String, String>> linuxProcess = splitStream
//                .select("Linux").map(new LinuxMapFunction());
//
//
//        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, String>> process1 = linuxProcess
//                .keyBy(1)
//                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS)).process(new LinuxProcessMapFunction());
//        SingleOutputStreamOperator<Tuple6<String,String, String, String, String, String>> alert_rule = process1.connect(alarmDataStream.broadcast(ALARM_RULES))
//                .process(new BroadcastProcessFunction<Tuple6<String,String,String, String, String, String>, Map<String, String>, Tuple6<String,String, String, String, String, String>>() {
//                    MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
//                            "alarm_rules",
//                            BasicTypeInfo.STRING_TYPE_INFO,
//                            BasicTypeInfo.STRING_TYPE_INFO);
//
//                    @Override
//                    public void processElement(Tuple6<String,String,String, String, String, String> value, ReadOnlyContext ctx, Collector<Tuple6<String,String,String, String, String, String>> out) throws Exception {
//                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
//                        String alter = value.f1;
//
//                        if (!broadcastState.contains(alter)) {
//                            return;
//                        }
//                        //unique_id + ":" + code + ":" + alarm;
//                        String targetId = broadcastState.get(alter);
//                        String[] split = targetId.split(":");
//                        if(split.length!=3){
//                            return;
//                        }
//                        String unique_id= split[0];
//                        String code= split[1];
//                        if(!code.equals(value.f2)){
//                            return;
//                        }
//                        String valu= split[2];
//                        if(unique_id.isEmpty()||code.isEmpty()||valu.isEmpty()){
//                            return;
//                        }
//                        String[] split1 = valu.split("\\|");
//                        if(split1.length!=3){
//                            return;
//                        }
//                        Double num_1=Double.parseDouble(split1[0]);
//                        Double num_2=Double.parseDouble(split1[1]);
//                        Double num_3=Double.parseDouble(split1[2]);
//                        double data_value = Double.parseDouble(value.f4);
//                        if((data_value>num_1||data_value==num_1)&&data_value<num_2){
//                            Tuple6<String,String,String, String, String, String> alter_message = Tuple6.of(value.f0, value.f1, value.f2, value.f3, "一级告警",unique_id);
//                            out.collect(alter_message);
//                        }else if((data_value>num_2||data_value==num_2)&&data_value<num_3){
//                            Tuple6<String,String,String, String, String, String> alter_message = Tuple6.of(value.f0, value.f1, value.f2, value.f3, "二级告警",unique_id);
//                            out.collect(alter_message);
//                        }else if(data_value>num_3||data_value==num_2){
//                            Tuple6<String,String,String, String, String, String> alter_message = Tuple6.of(value.f0, value.f1, value.f2, value.f3, "三级告警","DPComplete");
//                            out.collect(alter_message);
//                        }
//                    }
//
//                    @Override
//                    public void processBroadcastElement(Map<String, String> value, Context ctx, Collector<Tuple6<String,String,String, String, String, String>> out) throws Exception {
//                        if (value != null) {
//                            BroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
//                            for (Map.Entry<String, String> entry : value.entrySet()) {
//                                broadcastState.put(entry.getKey(), entry.getValue());
//                            }
//                        }
//                    }
//                });
//
//        //华三交换机指标处理
//        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, String>> h3c_switch = splitStream.select("H3C_Switch")
//                .map(new H3cSwitchMapFunction());
////        h3c_switch.print();
//        //华三交换机端口写入hbase中
//        h3c_switch.map(new WriteHbase());
//        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, String>> h3cSwitch = h3c_switch.keyBy(1)
//                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
//                .process(new H3CSwitchProcessMapFunction());
//        //中兴交换机指标处理
//        DataStream<Tuple6<String, String, String, String, String, String>> zx_switch = splitStream
//                .select("ZX_Switch").map(new ZxSwitchMapFunction());
//        //中兴交换机端口写入hbase
//        zx_switch.map(new WriteHbase());
//        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, String>> zxSwitch = zx_switch.keyBy(1)
//                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
//                .process(new ZXSwitchProcessMapFunction());
//        //DPI设备处理逻辑
//        DataStream<Tuple6<String, String, String, String, String, String>> dpi_stream = splitStream.select("DPI").map(new DpiSwitchMapFunction());
//        dpi_stream.map(new WriteHbase());
//        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, String>> dpi
//                = dpi_stream
//                .keyBy(1)
//                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
//                .process(new DPIProcessMapFunction());
//        //华为交换机指标处理
//        DataStream<Tuple6<String, String, String, String, String, String>> hw_switch = splitStream.select("HW_Switch");
//
//
//        String opentsdb_url = parameterTool.get("dtc.opentsdb.url", "http://10.10.58.16:4399");
////        process1.addSink(new SinkToOpentsdb(opentsdb_url)).name("Linux-Opentsdb-Sink");
//        h3cSwitch.addSink(new SinkToOpentsdb(opentsdb_url)).name("H3C-Opentsdb-Sink");
//        zxSwitch.addSink(new SinkToOpentsdb(opentsdb_url)).name("ZX-Opentsdb-Sink");
//        dpi.addSink(new SinkToOpentsdb(opentsdb_url)).name("DPI-Opentsdb-Sink");
//        env.execute("Snmp-Data-Process");
//
//    }
//
//
//}
//
//
//@Slf4j
//class MyMapFunctionV2 implements MapFunction<String, Tuple6<String, String, String, String, String, String>> {
//    //对json数据进行解析并且存入Tuple
//    @Override
//    public Tuple6<String, String, String, String, String, String> map(String s) {
//        if (s.isEmpty()) {
//            //判断数据是否是空
//            return Tuple6.of("null", "null", "null", "null", "null", "null");
//        }
//        if (!isJSON2(s)) {
//            //判断数据是否是json格式
//            return Tuple6.of("null", "null", "null", "null", "null", "null");
//        }
//        ObjectMapper objectMapper = new ObjectMapper();
//        JsonNode json = null;
//        try {
//            json = objectMapper.readTree(s);
//        } catch (IOException e) {
//            log.error("Data resolve make mistake,and the reason is " + e);
//            return Tuple6.of("null", "null", "null", "null", "null", "null");
//        }
//        String[] codes = json.get("code").textValue().split("_");
//        String system_name = codes[0].trim() + "_" + codes[1].trim();
//        String ZB_name = codes[0].trim() + "_" + codes[1].trim() + "_" + codes[2].trim() + "_" + codes[3].trim();
//        String ZB_code = codes[4].trim();
//        String time = json.get("time").textValue();
//        String value = json.get("value").textValue();
//        String host = json.get("host").textValue().trim();
//        return Tuple6.of(system_name, host, ZB_name, ZB_code, time, value);
//    }
//
//    public static boolean isJSON2(String str) {
//        boolean result = false;
//        try {
//            Object obj = JSON.parse(str);
//            result = true;
//        } catch (Exception e) {
//            log.warn("Event data is not musi");
//            result = false;
//        }
//        return result;
//    }
//}
//
//
//
//class WriteHbase extends RichMapFunction<Tuple6<String, String, String, String, String, String>, Tuple6<String, String, String, String, String, String>> {
//    ParameterTool parameter1;
//
//    @Override
//    public void open(org.apache.flink.configuration.Configuration parameters) {
//        parameter1 = (ParameterTool)
//                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
//    }
//
//    @Override
//    public Tuple6<String, String, String, String, String, String> map(Tuple6<String, String, String, String, String, String> event) throws Exception {
//        if ("102_101_103_107_108".equals(event.f2) || "102_103_103_110_110".equals(event.f2) || "103_102_103_106_106".equals(event.f2)) {
//            writeEventToHbase(event, parameter1);
//        }
//        return event;
//    }
//
//    private static void writeEventToHbase(Tuple6<String, String, String, String, String, String> event, ParameterTool parameterTool) throws IOException {
//        //表名
//        TableName HBASE_TABLE_NAME = TableName.valueOf(parameterTool.get("hbase.table.name"));
//        //列族
//        String INFO_STREAM = parameterTool.get("hbase.column.name");
//        Configuration configuration = HBaseConfiguration.create();
//        configuration.set(HBASE_ZOOKEEPER_QUORUM, parameterTool.get(HBASE_ZOOKEEPER_QUORUM));
//        configuration.set(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, parameterTool.get(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT));
//        configuration.set(HBASE_RPC_TIMEOUT, parameterTool.get(HBASE_RPC_TIMEOUT));
//        configuration.set(HBASE_CLIENT_OPERATION_TIMEOUT, parameterTool.get(HBASE_CLIENT_OPERATION_TIMEOUT));
//        configuration.set(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, parameterTool.get(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD));
//        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//
//        Connection connect = ConnectionFactory.createConnection(configuration);
//        Admin admin = connect.getAdmin();
//        //检查是否有该表，如果没有，创建
//        if (!admin.tableExists(HBASE_TABLE_NAME)) {
//            admin.createTable(new HTableDescriptor(HBASE_TABLE_NAME).addFamily(new HColumnDescriptor(INFO_STREAM)));
//        }
//        Table table = connect.getTable(HBASE_TABLE_NAME);
//        Put put = new Put(Bytes.toBytes(event.f1 + ":" + event.f3));
//        put.addColumn(Bytes.toBytes(INFO_STREAM), Bytes.toBytes("value"), Bytes.toBytes(event.f3));
//        table.put(put);
//        table.close();
//        connect.close();
//    }
//}
//
