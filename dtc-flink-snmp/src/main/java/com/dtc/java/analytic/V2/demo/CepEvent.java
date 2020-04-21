package com.dtc.java.analytic.V2.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * Created on 2019-10-21
 *
 * @author :hao.li
 */
@Slf4j
public class CepEvent {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<Tuple3<Integer, String, String>> eventStream = env.fromElements(
//                Tuple3.of(1400, "login1", "fail"),
//                Tuple3.of(1500, "login1", "fail"),
//                Tuple3.of(1310, "login2", "fail"),
//                Tuple3.of(1600, "login1", "fail"),
//                Tuple3.of(1320, "login2", "fail"),
//                Tuple3.of(1330, "login2", "fail"),
//                Tuple3.of(1450, "exit", "success"),
//                Tuple3.of(1340, "login2", "fail"),
//                Tuple3.of(982, "login", "fail"));
        DataStreamSource<String> dataStreamSource = env.socketTextStream("192.168.1.5", 8080, '\n');
        SingleOutputStreamOperator<Tuple3<Integer, String, String>> eventStream = dataStreamSource.map(new MyMapFunction());
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipToFirst("begin");
        Pattern<Tuple3<Integer, String, String>, ?> loginFail =
                Pattern.<Tuple3<Integer, String, String>>begin("begin", skipStrategy)
                        .where(new SimpleCondition<Tuple3<Integer, String, String>>() {
                            @Override
                            public boolean filter(Tuple3<Integer, String, String> integerStringStringTuple3) throws Exception {
                                return false;
                            }

                            @Override
                            public boolean filter(Tuple3<Integer, String, String> s,
                                                  Context<Tuple3<Integer, String, String>> context) throws Exception {
                                System.out.println("DPComplete");
                                return s.f2.equalsIgnoreCase("fail");
                            }
                        }).times(3).within(Time.seconds(30));
        PatternStream<Tuple3<Integer, String, String>> patternStream =
                CEP.pattern(eventStream.keyBy(x -> x.f1), loginFail);
        DataStream<String> alarmStream =
                patternStream.select(new PatternSelectFunction<Tuple3<Integer, String, String>, String>() {
                    @Override
                    public String select(Map<String, List<Tuple3<Integer, String, String>>> map) throws Exception {
//                        log.info("p = {}", map);
                        System.out.println("p = {},"+map);
                        String msg = String.format("ID %d has login failed 3 times in 5 seconds.and User %s"
                                , map.values().iterator().next().get(2).f0, map.values().iterator().next().get(2).f1);
                        return msg;
                    }
                });

        alarmStream.print();

        env.execute("cep event DPComplete");
    }
}

@Slf4j
class MyMapFunction implements MapFunction<String, Tuple3<Integer,String,String>> {
    @Override
    public Tuple3<Integer,String,String> map(String sourceEvent) {
        String[] split = sourceEvent.split(",");

        return Tuple3.of(Integer.parseInt(split[0]),split[1],split[2]);
    }
}

