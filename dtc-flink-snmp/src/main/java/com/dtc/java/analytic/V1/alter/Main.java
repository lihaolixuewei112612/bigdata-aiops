package com.dtc.java.analytic.V1.alter;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Created on 2019-12-30
 *
 * @author :hao.li
 */
@Slf4j
public class Main {

    final static MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
            "alarm_rules",
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO);

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        Map<String, String> stringStringMap = parameterTool.toMap();
        Properties properties = new Properties();
        for (String key : stringStringMap.keySet()) {
            if (key.startsWith("mysql")) {
                properties.setProperty(key, stringStringMap.get(key));
            }
        }

        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        DataStreamSource<Map<String, String>> alarmDataStream = env.addSource(new GetAlarmNotifyData()).setParallelism(1);//数据流定时从数据库中查出来数据
        alarmDataStream.print();

        SingleOutputStreamOperator<Tuple5<String,String, String, String, String>> map = env.addSource(new MySourceEvent()).map(new MyMapFunctionV2());

        SingleOutputStreamOperator<Tuple5<String, String, String, String, String>> alert_rule = map.connect(alarmDataStream.broadcast(ALARM_RULES))
                .process(new BroadcastProcessFunction<Tuple5<String,String, String, String, String>, Map<String, String>, Tuple5<String, String, String, String, String>>() {
                    MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
                            "alarm_rules",
                            BasicTypeInfo.STRING_TYPE_INFO,
                            BasicTypeInfo.STRING_TYPE_INFO);

                    @Override
                    public void processElement(Tuple5<String,String, String, String, String> value, ReadOnlyContext ctx, Collector<Tuple5<String, String, String, String, String>> out) throws Exception {
                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
                        String alter = value.f1;

                        if (!broadcastState.contains(alter)) {
                            return;
                        }
                        //unique_id + ":" + code + ":" + alarm;
                        String targetId = broadcastState.get(alter);
                        String[] split = targetId.split(":");
                        if(split.length!=3){
                            return;
                        }
                        String unique_id= split[0];
                        String code= split[1];
                        if(!code.equals(value.f2)){
                            return;
                        }
                        String valu= split[2];
                        if(unique_id.isEmpty()||code.isEmpty()||valu.isEmpty()){
                            return;
                        }
                        String[] split1 = valu.split("\\|");
                        if(split1.length!=3){
                            return;
                        }
                        Double num_1=Double.parseDouble(split1[0]);
                        Double num_2=Double.parseDouble(split1[1]);
                        Double num_3=Double.parseDouble(split1[2]);
                        double data_value = Double.parseDouble(value.f4);
                        if((data_value>num_1||data_value==num_1)&&data_value<num_2){
                            Tuple5<String, String, String, String, String> alter_message = Tuple5.of(value.f0, value.f1, value.f2, value.f3, "一级告警");
                            out.collect(alter_message);
                        }else if((data_value>num_2||data_value==num_2)&&data_value<num_3){
                            Tuple5<String, String, String, String, String> alter_message = Tuple5.of(value.f0, value.f1, value.f2, value.f3, "二级告警");
                            out.collect(alter_message);
                        }else if(data_value>num_3||data_value==num_2){
                            Tuple5<String, String, String, String, String> alter_message = Tuple5.of(value.f0, value.f1, value.f2, value.f3, "三级告警");
                            out.collect(alter_message);
                        }
                    }

                    @Override
                    public void processBroadcastElement(Map<String, String> value, Context ctx, Collector<Tuple5<String, String, String, String, String>> out) throws Exception {
                        if (value != null) {
                            BroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
                            for (Map.Entry<String, String> entry : value.entrySet()) {
                                broadcastState.put(entry.getKey(), entry.getValue());
                            }
                        }
                    }
                });

//        alert_rule.addSink(new MysqlSink(properties));
//        alert_rule.print("alter message");
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipToFirst("begin");
//
        Pattern<Tuple5<String, String, String, String, String>, ?> loginFail =
                Pattern.<Tuple5<String, String, String, String, String>>begin("begin", skipStrategy)
                        .where(new IterativeCondition<Tuple5<String, String, String, String, String>>() {
                            @Override
                            public boolean filter(Tuple5<String, String, String, String, String> s,
                                                  Context<Tuple5<String, String, String, String, String>> context) {
                                System.out.println("lihao:" + s.toString());
                                return s.f4.equalsIgnoreCase("一级告警");
                            }
                        }).optional().oneOrMore().within(Time.seconds(3));
        PatternStream<Tuple5<String, String, String, String, String>> patternStream =
                CEP.pattern(alert_rule.keyBy(x -> x.f1), loginFail);
        DataStream<String> alarmStream =
                patternStream.select(new PatternSelectFunction<Tuple5<String, String, String, String, String>, String>() {
                    @Override
                    public String select(Map<String, List<Tuple5<String, String, String, String, String>>> map) throws Exception {
                        log.info("1 ===================================================================");
//                      log.info("p = {}", map);
                        System.out.println("p = {}," + map);
                        String msg = String.format("ID %d has login failed 3 times in 5 seconds.and User %s"
                                , map.values().iterator().next().get(0).f0, map.values().iterator().next().get(0).f1);
                        return msg;
                    }
                });
//
        alarmStream.print("alater message.....");


//        SingleOutputStreamOperator<MetricEvent> filter = alert.filter(e -> {
//            String desc = e.getTags().get("desc");
//            log.info("10**************"+desc);
//            if (desc.equals("usb插入")) {
//                return false;
//            }else {
//                return true;
//            }
//        });
//        filter.map(e->e.getName()).print("finally");
//
//        //其他的业务逻辑
//        //alert.
//
//        //然后在下游的算子中有使用到 alarmNotifyMap 中的配置信息


        env.execute("zhisheng broadcast demo");
    }

    @Slf4j
    static class MyMapFunctionV2 implements MapFunction<String, Tuple5<String,String, String, String, String>> {
        //对json数据进行解析并且存入Tuple
        @Override
        public Tuple5<String,String, String, String, String> map(String s) {
            if (s.isEmpty()) {
                //判断数据是否是空
                return Tuple5.of("null","null", "null", "null", "null");
            }
            if (!isJSON2(s)) {
                //判断数据是否是json格式
                return Tuple5.of("null","null", "null", "null", "null");
            }
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode json = null;
            try {
                json = objectMapper.readTree(s);
            } catch (IOException e) {
                log.error("Data resolve make mistake,and the reason is " + e);
                return Tuple5.of("null","null", "null", "null", "null");
            }
            String codes = json.get("code").textValue();
            String time = json.get("time").textValue();
            String value = json.get("value").textValue();
            String host = json.get("host").textValue().trim();
            return Tuple5.of("123",host, codes, time, value);
        }

        boolean isJSON2(String str) {
            boolean result = false;
            try {
                Object obj = JSON.parse(str);
                result = true;
            } catch (Exception e) {
                log.warn("Event data is not musi");
                result = false;
            }
            return result;
        }
    }
}
