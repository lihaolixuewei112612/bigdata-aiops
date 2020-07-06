package com.dtc.java.analytic.V2.worker;

import com.dtc.java.analytic.V2.common.model.AlterStruct;
import com.dtc.java.analytic.V2.common.model.DataStruct;
import com.dtc.java.analytic.V2.common.model.SourceEvent;
import com.dtc.java.analytic.V2.common.utils.ExecutionEnvUtil;
import com.dtc.java.analytic.V2.map.function.LinuxMapFunction;
import com.dtc.java.analytic.V2.map.function.WinMapFunction;
import com.dtc.java.analytic.V2.process.function.LinuxProcessMapFunction;
import com.dtc.java.analytic.V2.process.function.WinProcessMapFunction;
import com.dtc.java.analytic.V2.process.function.alarmConvergence;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * Created on 2019-08-12
 *
 * @author :ren
 */
public class StreamToFlinkV3Test {
    private static final Logger logger = LoggerFactory.getLogger(StreamToFlinkV3.class);
    private static DataStream<Map<String, String>> alarmDataStream = null;

    public static void main(String[] args) throws Exception {
        MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
                "alarm_rules",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        String opentsdb_url = parameterTool.get("dtc.opentsdb.url", "http://10.10.58.16:4399");
        int windowSizeMillis = parameterTool.getInt("dtc.windowSizeMillis", 2000);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.getConfig().setGlobalJobParameters(parameterTool);
        DataStreamSource<Tuple9<String, String, String, String, Double, String, String, String, String>> alarmMessageMysql = env.addSource(new TestSourceEvent()).setParallelism(1);
        DataStream<Map<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>>> process = alarmMessageMysql.keyBy(0, 5).timeWindow(Time.milliseconds(windowSizeMillis)).process(new MySqlProcessMapFunction());
        alarmDataStream = process.map(new MySQLFunction());
        BroadcastStream<Map<String, String>> broadcast = alarmDataStream.broadcast(ALARM_RULES);

        DataStreamSource<SourceEvent> streamSource = env.addSource(new KafkaTestSourceEvent());

        /**
         * {"time":"1581691002687","code":"101_101_107_105_105","host":"10.3.7.234","nameCN":"磁盘剩余大小","value":"217802544","nameEN":"disk_free"}
         * */
//        DataStreamSource<String> dataStreamSource = env.socketTextStream("172.20.10.2", 8080, '\n');

        SingleOutputStreamOperator<DataStruct> mapStream = streamSource.map(new MyMapFunctionV3());
////        SingleOutputStreamOperator<DataStruct> timeSingleOutputStream
////                = mapStream.assignTimestampsAndWatermarks(new DtcPeriodicAssigner());
//
        SplitStream<DataStruct> splitStream
                = mapStream.split((OutputSelector<DataStruct>) event -> {
            List<String> output = new ArrayList<>();
            String type = event.getSystem_name();
            if ("101_100".equals(type) | "107_107".equals(type)) {
                output.add("Win");
            } else if ("101_101".equals(type)) {
                output.add("Linux");
            } else if ("102_101".equals(type)) {
                output.add("H3C_Switch");
            } else if ("102_102".equals(type)) {
                output.add("HW_Switch");
            } else if ("102_103".equals(type)) {
                output.add("ZX_Switch");
            } else if ("103_102".equals(type)) {
                output.add("DPI");
            }
            return output;
        });
        //windows指标数据处理
        SingleOutputStreamOperator<DataStruct> winProcess = splitStream
                .select("Win")
                .map(new WinMapFunction())
                .keyBy("Host")
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
                .process(new WinProcessMapFunction());
        //windows数据全量写opentsdb
//        winProcess.addSink(new PSinkToOpentsdb(opentsdb_url));

        //windows数据进行告警规则判断并将告警数据写入mysql
        List<DataStream<AlterStruct>> alarmWindows = getAlarm(winProcess, broadcast);
//        alarmWindows.forEach(e-> {
//           e.keyBy("gaojing")
//                    .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
//                    .process(new alarmConvergence()).print("告警收敛信息打印：   ");
//        });

        //linux指标数据处理
        SingleOutputStreamOperator<DataStruct> linuxProcess = splitStream
                .select("Linux")
                .map(new LinuxMapFunction())
                .keyBy("Host")
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
                .process(new LinuxProcessMapFunction());

        //Linux数据全量写opentsdb
//        linuxProcess.addSink(new PSinkToOpentsdb(opentsdb_url));
//
        //Linux数据进行告警规则判断并将告警数据写入mysql
        List<DataStream<AlterStruct>> alarmLinux = getAlarm(linuxProcess, broadcast);
        alarmLinux.forEach(e -> e.keyBy(new KeySelector<AlterStruct, String>() {
            @Override
            public String getKey(AlterStruct value) throws Exception {
                return value.getGaojing();
            }
            //时间窗口 6秒  滑动间隔3秒
        }).timeWindow(Time.seconds(60))
                .aggregate(new CountAggregate_2(), new CountWindowFunction_2()).print("test:"));
        alarmLinux.forEach(e -> e.print("linux打印告警:"));

//        alarmLinux.forEach(e -> e.addSink(new MysqlSink()));
        env.execute("Dtc-Alarm-Flink-Process");
    }

    public static class CountWindowFunction_2 implements WindowFunction<Tuple4<AlterStruct, Double, Double, Double>, String, String, TimeWindow> {
        @Override
        public void apply(String productId, TimeWindow window, Iterable<Tuple4<AlterStruct, Double, Double, Double>> input, Collector<String> out) throws Exception {
            /*商品访问统计输出*/
            /*out.collect("productId"productId,window.getEnd(),input.iterator().next()));*/
            out.collect("----------------窗口时间：" + window.getEnd());
            out.collect("商品ID: " + productId + " 个数是: " + input.iterator().next().f1 + "  浏览量: " + input.iterator().next().f2 + "   值是： " + input.iterator().next().f3);
        }
    }

    public static class CountAggregate_2 implements AggregateFunction<AlterStruct, Tuple3<AlterStruct, Double, Double>, Tuple4<AlterStruct, Double, Double, Double>> {
        @Override
        public Tuple3 createAccumulator() {
            /*访问量初始化为0*/
            return Tuple3.of("", 0D, 0D);
        }

        @Override
        public Tuple3<AlterStruct, Double, Double> add(AlterStruct value, Tuple3<AlterStruct, Double, Double> acc) {
            /*访问量直接+1 即可*/
            return new Tuple3<>(value, acc.f1 + Double.parseDouble(value.getValue()), acc.f2 + 1);
        }

        @Override
        public Tuple4<AlterStruct, Double, Double, Double> getResult(Tuple3<AlterStruct, Double, Double> acc) {
            Double result = acc.f1 / acc.f2;
            return Tuple4.of(acc.f0, acc.f1, acc.f2, result);
        }

        @Override
        public Tuple3<AlterStruct, Double, Double> merge(Tuple3<AlterStruct, Double, Double> longLongTuple2, Tuple3<AlterStruct, Double, Double> acc1) {
            return new Tuple3<>(longLongTuple2.f0, longLongTuple2.f1 + acc1.f1, longLongTuple2.f2 + acc1.f2);
        }
    }

    private static List<DataStream<AlterStruct>> getAlarm(SingleOutputStreamOperator<DataStruct> event, BroadcastStream<Map<String, String>> broadcast) {

        SingleOutputStreamOperator<AlterStruct> alert_rule = event.connect(broadcast)
                .process(getAlarmFunction());

//        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipToFirst("begin");
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        Pattern<AlterStruct, ?> alarmGrade =
                Pattern.<AlterStruct>begin("begin", skipStrategy).subtype(AlterStruct.class)
                        .where(new SimpleCondition<AlterStruct>() {
                            @Override
                            public boolean filter(AlterStruct s) {
                                return s.getLevel().equals("1");
                            }
                        }).or(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct s) {
                        return s.getLevel().equals("2");
                    }
                }).or(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct s) {
                        return s.getLevel().equals("3");
                    }
                }).or(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct s) {
                        return s.getLevel().equals("4");
                    }
                }).times(3).within(Time.seconds(20));
        Pattern<AlterStruct, ?> alarmIncream
                = Pattern.<AlterStruct>begin("begin", skipStrategy).subtype(AlterStruct.class)
                .where(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct alterStruct) {
                        return alterStruct.getLevel().equals("1");
                    }
                }).next("middle").where(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct alterStruct) {
                        return alterStruct.getLevel().equals("2");
                    }
                }).next("three").where(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct alterStruct) {
                        return alterStruct.getLevel().equals("3");
                    }
                }).next("finally").where(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct alterStruct) {
                        return alterStruct.getLevel().equals("4");
                    }
                }).times(2).within(Time.seconds(20));
        PatternStream<AlterStruct> patternStream =
                CEP.pattern(alert_rule.keyBy(AlterStruct::getGaojing), alarmGrade);
        PatternStream<AlterStruct> alarmIncreamStream =
                CEP.pattern(alert_rule.keyBy(AlterStruct::getGaojing), alarmIncream);
        DataStream<AlterStruct> alarmStream =
                patternStream.select(new PatternSelectFunction<AlterStruct, AlterStruct>() {
                    @Override
                    public AlterStruct select(Map<String, List<AlterStruct>> map) throws Exception {
                        return map.values().iterator().next().get(0);
                    }
                });
        DataStream<AlterStruct> alarmStreamIncream =
                alarmIncreamStream.select(new PatternSelectFunction<AlterStruct, AlterStruct>() {
                    @Override
                    public AlterStruct select(Map<String, List<AlterStruct>> map) throws Exception {
                        return map.values().iterator().next().get(2);
                    }
                });
        List<DataStream<AlterStruct>> list = new ArrayList<>();
        list.add(alarmStream);
        list.add(alarmStreamIncream);
        return list;
    }

    private static BroadcastProcessFunction<DataStruct, Map<String, String>, AlterStruct> getAlarmFunction() {
        return new BroadcastProcessFunction<DataStruct, Map<String, String>, AlterStruct>() {
            MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
                    "alarm_rules",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO);

            @Override
            public void processElement(DataStruct value, ReadOnlyContext ctx, Collector<AlterStruct> out) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
                String host_ip = value.getHost();
                String code = value.getZbFourName().replace("_", ".");
                String weiyi = host_ip + "." + code;
                if (!broadcastState.contains(weiyi)) {
                    return;
                }
                //asset_id + ":" + code + ":"+ asset_code + ":" +asset_name+":"+ alarm
                String targetId = broadcastState.get(weiyi).trim();
                String[] split = targetId.split(":");
                if (split.length != 5) {
                    return;
                }
                String unique_id = split[0].trim();
                String r_code = split[1].trim();
                if (!r_code.equals(value.getZbFourName())) {
                    return;
                }
                String asset_code = split[2].trim();
                String asset_name = split[3].trim();
                String result = asset_code + "(" + asset_name + ")";
                String r_value = split[4].trim();
                if (unique_id.isEmpty() || code.isEmpty() || r_value.isEmpty()) {
                    return;
                }
                String[] split1 = r_value.split("\\|");
                if (split1.length != 4) {
                    return;
                }
                broadcastState.clear();
                AlarmRule(value, out, unique_id, split1, result);
            }

        @Override
        public void processBroadcastElement (Map < String, String > value, Context ctx, Collector < AlterStruct > out) throws
        Exception {
            if (value == null || value.size() == 0) {
                return;
            }
            BroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
            for (Map.Entry<String, String> entry : value.entrySet()) {
                broadcastState.put(entry.getKey(), entry.getValue());
            }
        }
    }

    ;
}

    /**
     * 告警规则
     */
    private static void AlarmRule(DataStruct value, Collector<AlterStruct> out, String unique_id, String[] split1, String str1) {
        double data_value = Double.parseDouble(value.getValue());
        String code_name = str1;
        String level_1 = split1[0];
        String level_2 = split1[1];
        String level_3 = split1[2];
        String level_4 = split1[3];
        //四个阈值都不为空
        if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_3)) && !("null".equals(level_4))) {
            Double num_1 = Double.parseDouble(split1[0]);
            Double num_2 = Double.parseDouble(split1[1]);
            Double num_3 = Double.parseDouble(split1[2]);
            Double num_4 = Double.parseDouble(split1[3]);
            if ((data_value > num_1 || data_value == num_1) && data_value < num_2) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "1", unique_id, String.valueOf(num_1), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            } else if ((data_value > num_2 || data_value == num_2) && data_value < num_3) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "2", unique_id, String.valueOf(num_2), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            } else if ((data_value > num_3 || data_value == num_3) && data_value < num_4) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "3", unique_id, String.valueOf(num_3), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            } else if (data_value > num_4 || data_value == num_4) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "4", unique_id, String.valueOf(num_4), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            }
        }
        //一个不为空，其他的都为空
        else if (!("null".equals(level_1)) && "null".equals(level_2) && "null".equals(level_3) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split1[0]);
            if ((data_value > num_1 || data_value == num_1)) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "1", unique_id, String.valueOf(num_1), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            }
        } else if ("null".equals(level_1) && !("null".equals(level_2)) && "null".equals(level_3) && "null".equals(level_4)) {
            Double num_2 = Double.parseDouble(split1[1]);
            if ((data_value > num_2 || data_value == num_2)) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "2", unique_id, String.valueOf(num_2), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            }
        } else if ("null".equals(level_1) && "null".equals(level_2) && !("null".equals(level_3)) && "null".equals(level_4)) {
            Double num_3 = Double.parseDouble(split1[2]);
            if ((data_value > num_3 || data_value == num_3)) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "3", unique_id, String.valueOf(num_3), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            }
        } else if ("null".equals(level_1) && "null".equals(level_2) && "null".equals(level_3) && !("null".equals(level_4))) {
            Double num_4 = Double.parseDouble(split1[3]);
            if ((data_value > num_4 || data_value == num_4)) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "4", unique_id, String.valueOf(num_4), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            }
        }
        //两个为空，两个不为空
        else if (!("null".equals(level_1)) && !("null".equals(level_2)) && "null".equals(level_3) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split1[0]);
            Double num_2 = Double.parseDouble(split1[1]);
            if ((data_value > num_1 || data_value == num_1) && (data_value < num_2)) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "1", unique_id, String.valueOf(num_1), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            } else if ((data_value > num_2 || data_value == num_2)) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "2", unique_id, String.valueOf(num_2), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_3)) && "null".equals(level_2) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split1[0]);
            Double num_3 = Double.parseDouble(split1[2]);
            if ((data_value > num_1 || data_value == num_1) && (data_value < num_3)) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "1", unique_id, String.valueOf(num_1), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            } else if ((data_value > num_3 || data_value == num_3)) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "3", unique_id, String.valueOf(num_3), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_4)) && "null".equals(level_2) && "null".equals(level_3)) {
            Double num_1 = Double.parseDouble(split1[0]);
            Double num_4 = Double.parseDouble(split1[3]);
            if ((data_value > num_1 || data_value == num_1) && (data_value < num_4)) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "1", unique_id, String.valueOf(num_1), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            } else if ((data_value > num_4 || data_value == num_4)) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "4", unique_id, String.valueOf(num_4), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_2)) && !("null".equals(level_3)) && "null".equals(level_1) && "null".equals(level_4)) {
            Double num_3 = Double.parseDouble(split1[2]);
            Double num_2 = Double.parseDouble(split1[1]);
            if ((data_value > num_2 || data_value == num_2) && (data_value < num_3)) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "2", unique_id, String.valueOf(num_2), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            } else if ((data_value > num_3 || data_value == num_3)) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "3", unique_id, String.valueOf(num_3), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_2)) && !("null".equals(level_4)) && "null".equals(level_1) && "null".equals(level_3)) {
            Double num_4 = Double.parseDouble(split1[3]);
            Double num_2 = Double.parseDouble(split1[1]);
            if ((data_value > num_2 || data_value == num_2) && (data_value < num_4)) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "2", unique_id, String.valueOf(num_2), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            } else if ((data_value > num_4 || data_value == num_4)) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "4", unique_id, String.valueOf(num_4), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_1) && "null".equals(level_2)) {
            Double num_4 = Double.parseDouble(split1[3]);
            Double num_3 = Double.parseDouble(split1[2]);
            if ((data_value > num_3 || data_value == num_3) && (data_value < num_4)) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "3", unique_id, String.valueOf(num_3), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            } else if ((data_value > num_4 || data_value == num_4)) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "4", unique_id, String.valueOf(num_4), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            }
        }
        //三不空，一空
        else if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_3)) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split1[0]);
            Double num_2 = Double.parseDouble(split1[1]);
            Double num_3 = Double.parseDouble(split1[2]);
            if ((data_value > num_1 || data_value == num_1) && data_value < num_2) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "1", unique_id, String.valueOf(num_1), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            } else if ((data_value > num_2 || data_value == num_2) && data_value < num_3) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "2", unique_id, String.valueOf(num_2), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            } else if (data_value > num_3 || data_value == num_3) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "3", unique_id, String.valueOf(num_3), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_4)) && "null".equals(level_3)) {
            Double num_1 = Double.parseDouble(split1[0]);
            Double num_2 = Double.parseDouble(split1[1]);
            Double num_4 = Double.parseDouble(split1[3]);
            if ((data_value > num_1 || data_value == num_1) && data_value < num_2) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "1", unique_id, String.valueOf(num_1), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            } else if ((data_value > num_2 || data_value == num_2) && data_value < num_4) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "2", unique_id, String.valueOf(num_2), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            } else if (data_value > num_4 || data_value == num_4) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "4", unique_id, String.valueOf(num_4), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_2)) {
            Double num_1 = Double.parseDouble(split1[0]);
            Double num_3 = Double.parseDouble(split1[2]);
            Double num_4 = Double.parseDouble(split1[3]);
            if ((data_value > num_1 || data_value == num_1) && data_value < num_3) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "1", unique_id, String.valueOf(num_1), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            } else if ((data_value > num_3 || data_value == num_3) && data_value < num_4) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "3", unique_id, String.valueOf(num_3), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            } else if (data_value > num_4 || data_value == num_4) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "4", unique_id, String.valueOf(num_4), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_2)) && !("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_1)) {
            Double num_2 = Double.parseDouble(split1[1]);
            Double num_3 = Double.parseDouble(split1[2]);
            Double num_4 = Double.parseDouble(split1[3]);
            if ((data_value > num_2 || data_value == num_2) && data_value < num_3) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "2", unique_id, String.valueOf(num_2), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            } else if ((data_value > num_3 || data_value == num_3) && data_value < num_4) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "3", unique_id, String.valueOf(num_3), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            } else if (data_value > num_4 || data_value == num_4) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "4", unique_id, String.valueOf(num_4), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            }
        }
    }

static class MySQLFunction implements MapFunction<Map<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>>, Map<String, String>> {
    //(445,10.3.1.6,101_101_106_103,50.0,null,null,null)

    @Override
    public Map<String, String> map(Map<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>> event) throws Exception {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>> entries : event.entrySet()) {
            Tuple9<String, String, String, Double, Double, Double, Double, String, String> value = entries.getValue();
            String key = entries.getKey();
            String asset_id = value.f0;
            String ip = value.f1;
            String code = value.f2;
            Double level_1 = value.f3;
            Double level_2 = value.f4;
            Double level_3 = value.f5;
            Double level_4 = value.f6;
            String asset_code = value.f7;
            String asset_name = value.f8;
            String str = asset_id + ":" + code + ":" + asset_code + ":" + asset_name + ":" + level_1 + "|" + level_2 + "|" + level_3 + "|" + level_4;
            map.put(key, str);
        }
        return map;
    }
}

@Slf4j
static class MySqlProcessMapFunction extends ProcessWindowFunction<Tuple9<String, String, String, String, Double, String, String, String, String>, Map<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>>, Tuple, TimeWindow> {
    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple9<String, String, String, String, Double, String, String, String, String>> iterable, Collector<Map<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>>> collector) throws Exception {
        Tuple9<String, String, String, Double, Double, Double, Double, String, String> tuple9 = new Tuple9<>();
        Map<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>> map = new HashMap<>();
        for (Tuple9<String, String, String, String, Double, String, String, String, String> sourceEvent : iterable) {
            String asset_id = sourceEvent.f0;
            String ip = sourceEvent.f1;
            Double num = sourceEvent.f4;
            String code = sourceEvent.f5;
            String level = sourceEvent.f6;
            tuple9.f0 = asset_id;
            tuple9.f1 = ip;
            tuple9.f2 = code;
            String key = ip + "." + code.replace("_", ".");
            if ("1".equals(level)) {
                tuple9.f3 = num;
            } else if ("2".equals(level)) {
                tuple9.f4 = num;
            } else if ("3".equals(level)) {
                tuple9.f5 = num;
            } else if ("4".equals(level)) {
                tuple9.f6 = num;
            }
            tuple9.f7 = sourceEvent.f7;
            tuple9.f8 = sourceEvent.f8;
            map.put(key, tuple9);
        }
        collector.collect(map);
    }

}
}


@Slf4j
class MyMapFunctionV3 implements MapFunction<SourceEvent, DataStruct> {
    @Override
    public DataStruct map(SourceEvent sourceEvent) {
        String[] codes = sourceEvent.getCode().split("_");
        String systemName = codes[0].trim() + "_" + codes[1].trim();
        String zbFourCode = systemName + "_" + codes[2].trim() + "_" + codes[3].trim();
        String zbLastCode = codes[4].trim();
        String nameCN = sourceEvent.getNameCN();
        String nameEN = sourceEvent.getNameEN();
        String time = sourceEvent.getTime();
        String value = sourceEvent.getValue();
        String host = sourceEvent.getHost();
        return new DataStruct(systemName, host, zbFourCode, zbLastCode, nameCN, nameEN, time, value);
    }
}

