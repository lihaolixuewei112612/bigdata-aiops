//package com.dtc.java.analytic.V2.worker;
//
//import com.dtc.java.analytic.V2.common.model.AlterStruct;
//import com.dtc.java.analytic.V2.common.model.DataStruct;
//import com.dtc.java.analytic.V2.common.model.SourceEvent;
//import com.dtc.java.analytic.V2.common.utils.ExecutionEnvUtil;
//import com.dtc.java.analytic.V2.common.utils.KafkaConfigUtil;
//import com.dtc.java.analytic.V2.map.function.LinuxMapFunction;
//import com.dtc.java.analytic.V2.map.function.WinMapFunction;
//import com.dtc.java.analytic.V2.process.function.LinuxProcessMapFunction;
//import com.dtc.java.analytic.V2.process.function.WinProcessMapFunction;
//import com.dtc.java.analytic.V2.sink.mysql.MysqlSink;
//import com.dtc.java.analytic.V2.sink.opentsdb.PSinkToOpentsdb;
//import com.dtc.java.analytic.V2.source.mysql.ReadAlarmMessage;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.state.BroadcastState;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.cep.CEP;
//import org.apache.flink.cep.PatternSelectFunction;
//import org.apache.flink.cep.PatternStream;
//import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
//import org.apache.flink.cep.pattern.Pattern;
//import org.apache.flink.cep.pattern.conditions.SimpleCondition;
//import org.apache.flink.streaming.api.collector.selector.OutputSelector;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.datastream.SplitStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.util.Collector;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//import java.util.concurrent.TimeUnit;
//
//
///**
// * Created on 2019-08-12
// *
// * @author :ren
// */
//@Slf4j
//public class StreamToFlinkV5 {
//    final static MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
//            "alarm_rules",
//            BasicTypeInfo.STRING_TYPE_INFO,
//            BasicTypeInfo.STRING_TYPE_INFO);
//    private static DataStreamSource<Map<String, String>> alarmDataStream = null;
//
//    public static void main(String[] args) throws Exception {
//
//        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
//        String opentsdb_url = parameterTool.get("dtc.opentsdb.url", "http://10.10.58.16:4399");
//        Map<String, String> stringStringMap = parameterTool.toMap();
//        Properties properties = new Properties();
//        for (String key : stringStringMap.keySet()) {
//            if (key.startsWith("mysql")) {
//                properties.setProperty(key, stringStringMap.get(key));
//            }
//        }
//        int windowSizeMillis = parameterTool.getInt("dtc.windowSizeMillis", 2000);
//        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
//        env.getConfig().setGlobalJobParameters(parameterTool);
//        alarmDataStream = env.addSource(new ReadAlarmMessage()).setParallelism(1);
////        DataStreamSource<SourceEvent> streamSource = env.addSource(new TestSourceEvent());
//        DataStreamSource<SourceEvent> streamSource = KafkaConfigUtil.buildSource(env);
//
//        /**
//         * {"time":"1581691002687","code":"101_101_107_105_105","host":"10.3.7.234","nameCN":"磁盘剩余大小","value":"217802544","nameEN":"disk_free"}
//         * */
////        DataStreamSource<String> dataStreamSource = env.socketTextStream("172.20.10.2", 8080, '\n');
//
//        SingleOutputStreamOperator<DataStruct> mapStream = streamSource.map(new MyMapFunctionV3());
////        SingleOutputStreamOperator<DataStruct> timeSingleOutputStream
////                = mapStream.assignTimestampsAndWatermarks(new DtcPeriodicAssigner());
//
//        SplitStream<DataStruct> splitStream
//                = mapStream.split((OutputSelector<DataStruct>) event -> {
//            List<String> output = new ArrayList<>();
//            String type = event.getSystem_name();
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
//        SingleOutputStreamOperator<DataStruct> winProcess = splitStream
//                .select("Win")
//                .map(new WinMapFunction())
//                .keyBy("Host")
//                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
//                .process(new WinProcessMapFunction());
//        winProcess.print("widows: ");
//        //windows数据全量写opentsdb
//        winProcess.addSink(new PSinkToOpentsdb(opentsdb_url));
//
//        //windows数据进行告警规则判断并将告警数据写入mysql
//        List<DataStream<AlterStruct>> alarmWindows = getAlarm(winProcess);
//        alarmWindows.forEach(e -> e.addSink(new MysqlSink(properties)));
//
//        //linux指标数据处理
//        SingleOutputStreamOperator<DataStruct> linuxProcess = splitStream
//                .select("Linux")
//                .map(new LinuxMapFunction())
//                .keyBy("Host")
//                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
//                .process(new LinuxProcessMapFunction());
//        linuxProcess.print("111--:");
//
//        //Linux数据全量写opentsdb
//        linuxProcess.addSink(new PSinkToOpentsdb(opentsdb_url));
//
//        //Linux数据进行告警规则判断并将告警数据写入mysql
//        List<DataStream<AlterStruct>> alarmLinux = getAlarm(linuxProcess);
//        alarmLinux.forEach(e->e.print("2222---:"));
//        alarmLinux.forEach(e -> e.addSink(new MysqlSink(properties)));
//        //告警数据实时发送kafka
//        env.execute("Dtc-Alarm-Flink-Process");
//    }
//
//    private static List<DataStream<AlterStruct>> getAlarm(SingleOutputStreamOperator<DataStruct> event) {
//        SingleOutputStreamOperator<AlterStruct> alert_rule = event.connect(alarmDataStream.broadcast(ALARM_RULES))
//                .process(getAlarmFunction());
//
////        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipToFirst("begin");
//        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
//        Pattern<AlterStruct, ?> alarmGrade =
//                Pattern.<AlterStruct>begin("begin", skipStrategy).subtype(AlterStruct.class)
//                        .where(new SimpleCondition<AlterStruct>() {
//                            @Override
//                            public boolean filter(AlterStruct s) {
//                                return s.getLevel().equals("一级告警");
//                            }
//                        }).or(new SimpleCondition<AlterStruct>() {
//                    @Override
//                    public boolean filter(AlterStruct s) {
//                        return s.getLevel().equals("二级告警");
//                    }
//                }).or(new SimpleCondition<AlterStruct>() {
//                    @Override
//                    public boolean filter(AlterStruct s) {
//                        return s.getLevel().equals("三级告警");
//                    }
//                }).times(3).within(Time.seconds(10));
//        Pattern<AlterStruct, ?> alarmIncream
//                = Pattern.<AlterStruct>begin("begin", skipStrategy).subtype(AlterStruct.class)
//                .where(new SimpleCondition<AlterStruct>() {
//                    @Override
//                    public boolean filter(AlterStruct alterStruct) {
//                        return alterStruct.getLevel().equals("一级告警");
//                    }
//                }).next("middle").where(new SimpleCondition<AlterStruct>() {
//                    @Override
//                    public boolean filter(AlterStruct alterStruct) {
//                        return alterStruct.getLevel().equals("二级告警");
//                    }
//
//                }).next("finally").where(new SimpleCondition<AlterStruct>() {
//                    @Override
//                    public boolean filter(AlterStruct alterStruct) {
//                        return alterStruct.getLevel().equals("三级告警");
//                    }
//
//                }).times(2).within(Time.seconds(12));
//        PatternStream<AlterStruct> patternStream =
//                CEP.pattern(alert_rule.keyBy(AlterStruct::getGaojing), alarmGrade);
//        PatternStream<AlterStruct> alarmIncreamStream =
//                CEP.pattern(alert_rule.keyBy(AlterStruct::getGaojing), alarmIncream);
//        DataStream<AlterStruct> alarmStream =
//                patternStream.select(new PatternSelectFunction<AlterStruct, AlterStruct>() {
//                    @Override
//                    public AlterStruct select(Map<String, List<AlterStruct>> map) throws Exception {
//                        return map.values().iterator().next().get(0);
//                    }
//                });
//        DataStream<AlterStruct> alarmStreamIncream =
//                alarmIncreamStream.select(new PatternSelectFunction<AlterStruct, AlterStruct>() {
//                    @Override
//                    public AlterStruct select(Map<String, List<AlterStruct>> map) throws Exception {
//                        return map.values().iterator().next().get(2);
//                    }
//                });
//        List<DataStream<AlterStruct>> list = new ArrayList<>();
//        list.add(alarmStream);
//        list.add(alarmStreamIncream);
//        return list;
//    }
//
//    private static BroadcastProcessFunction<DataStruct, Map<String, String>, AlterStruct> getAlarmFunction() {
//        return new BroadcastProcessFunction<DataStruct, Map<String, String>, AlterStruct>() {
//            MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
//                    "alarm_rules",
//                    BasicTypeInfo.STRING_TYPE_INFO,
//                    BasicTypeInfo.STRING_TYPE_INFO);
//
//            @Override
//            public void processElement(DataStruct value, ReadOnlyContext ctx, Collector<AlterStruct> out) throws Exception {
//                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
//                String host_ip = value.getHost();
//                String code = value.getZbFourName().replace("_", ".");
//                String weiyi = host_ip + "." + code;
//                if (!broadcastState.contains(weiyi)) {
//                    return;
//                }
//                //unique_id + ":" + code + ":" + alarm;
//                String targetId = broadcastState.get(weiyi);
//                String[] split = targetId.split(":");
//                if (split.length != 3) {
//                    return;
//                }
//                String unique_id = split[0];
//                String r_code = split[1];
//                if (!r_code.equals(value.getZbFourName())) {
//                    return;
//                }
//                String r_value = split[2];
//                if (unique_id.isEmpty() || code.isEmpty() || r_value.isEmpty()) {
//                    return;
//                }
//                String[] split1 = r_value.split("\\|");
//                if (split1.length != 4) {
//                    return;
//                }
//                Double num_1 = Double.parseDouble(split1[0]);
//                Double num_2 = Double.parseDouble(split1[1]);
//                Double num_3 = Double.parseDouble(split1[2]);
//                double data_value = Double.parseDouble(value.getValue());
//                if ((data_value > num_1 || data_value == num_1) && data_value < num_2) {
//                    String system_time = String.valueOf(System.currentTimeMillis());
//                    AlterStruct alter_message = new AlterStruct(value.getSystem_name(), value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "一级告警", unique_id, String.valueOf(num_1), value.getHost() + "-" + value.getZbFourName());
//                    out.collect(alter_message);
//                } else if ((data_value > num_2 || data_value == num_2) && data_value < num_3) {
//                    String system_time = String.valueOf(System.currentTimeMillis());
//                    AlterStruct alter_message = new AlterStruct(value.getSystem_name(), value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "二级告警", unique_id, String.valueOf(num_2), value.getHost() + "-" + value.getZbFourName());
//                    out.collect(alter_message);
//                } else if (data_value > num_3 || data_value == num_2) {
//                    String system_time = String.valueOf(System.currentTimeMillis());
//                    AlterStruct alter_message = new AlterStruct(value.getSystem_name(), value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "三级告警", unique_id, String.valueOf(num_3), value.getHost() + "-" + value.getZbFourName());
//                    out.collect(alter_message);
//                }
//            }
//
//            @Override
//            public void processBroadcastElement(Map<String, String> value, Context ctx, Collector<AlterStruct> out) throws Exception {
//                if (value != null) {
//                    BroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
//                    for (Map.Entry<String, String> entry : value.entrySet()) {
//                        broadcastState.put(entry.getKey(), entry.getValue());
//                    }
//                }
//            }
//        };
//    }
//}
//
//
//@Slf4j
//class MyMapFunctionV5 implements MapFunction<SourceEvent, DataStruct> {
//    @Override
//    public DataStruct map(SourceEvent sourceEvent) {
//        String[] codes = sourceEvent.getCode().split("_");
//        String systemName = codes[0].trim() + "_" + codes[1].trim();
//        String zbFourCode = systemName + "_" + codes[2].trim() + "_" + codes[3].trim();
//        String zbLastCode = codes[4].trim();
//        String nameCN = sourceEvent.getNameCN();
//        String nameEN = sourceEvent.getNameEN();
//        String time = sourceEvent.getTime();
//        String value = sourceEvent.getValue();
//        String host = sourceEvent.getHost();
//        return new DataStruct(systemName, host, zbFourCode, zbLastCode, nameCN, nameEN, time, value);
//    }
//}
