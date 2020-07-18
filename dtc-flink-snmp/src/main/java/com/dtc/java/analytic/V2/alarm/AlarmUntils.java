package com.dtc.java.analytic.V2.alarm;

import com.dtc.java.analytic.V2.common.model.AlterStruct;
import com.dtc.java.analytic.V2.common.model.DataStruct;
import com.dtc.java.analytic.V2.common.model.TimesConstats;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Author : lihao
 * Created on : 2020-05-22
 * @Description : TODO描述类作用
 */
public class AlarmUntils {
    public static List<DataStream<AlterStruct>> getAlarm(SingleOutputStreamOperator<DataStruct> event, BroadcastStream<Map<String, String>> broadcast, TimesConstats test) {

        SingleOutputStreamOperator<AlterStruct> alert_rule = event.filter(e->!("107_107_101_101_101".equals(e.getZbFourName()))).connect(broadcast)
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
                }).times(test.getOne()).within(Time.seconds(test.getTwo()));
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
                }).times(test.getThree()).within(Time.seconds(test.getFour()));
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
//                broadcastState.clear();
                AlarmRule(value, out, unique_id, split1, result);
            }

            @Override
            public void processBroadcastElement(Map<String, String> value, Context ctx, Collector<AlterStruct> out) throws Exception {
                if (value == null || value.size() == 0) {
                    return;
                }
                if (value != null) {
                    BroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
                    for (Map.Entry<String, String> entry : value.entrySet()) {
                        broadcastState.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        };
    }
    /**
     * 告警规则
     */
    public static void AlarmRule(DataStruct value, Collector<AlterStruct> out, String unique_id, String[] split1, String str1) {
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
}
