package com.dtc.java.SC.JSC.exec;

import com.dtc.java.SC.JKZL.ExecutionEnvUtil;
import com.dtc.java.SC.JSC.source.JSC_AllNum;
import com.dtc.java.SC.JSC.source.JSC_ZCAllNum;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Properties;

/**
 * @Author : lihao
 * Created on : 2020-03-31
 * @Description : 驾驶舱监控大盘--设备总数/正常设备数/不正常设备数
 */
public class Alter_Num {
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
        int windowSizeMillis = 6000;
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        /**各机房各区域各机柜设备总数*/
        DataStreamSource<Tuple2<Integer, Integer>> tuple2DataStreamSource = env.addSource(new JSC_AllNum()).setParallelism(1);
        DataStreamSource<Tuple2<Integer, Integer>> tuple2DataStreamSource1 = env.addSource(new JSC_ZCAllNum()).setParallelism(1);
        DataStream<Tuple2<Integer, Integer>> tuple2DataStream = JKSB_Result_CGroup(tuple2DataStreamSource, tuple2DataStreamSource1, windowSizeMillis);
        tuple2DataStream.map(new JKSB_MyMapFunctionV()).print();
        env.execute("com.dtc.java.SC sart");
    }
    @Slf4j
    static class JKSB_MyMapFunctionV implements MapFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer,Integer>> {
        @Override
        public Tuple3<Integer, Integer,Integer> map(Tuple2<Integer, Integer> sourceEvent) {
            Integer All_Num = sourceEvent.f0;
            Integer ZC_Num = sourceEvent.f1;
            int BZC_Num = All_Num-ZC_Num;

            return Tuple3.of(All_Num,ZC_Num,BZC_Num);
        }
    }
    private static DataStream<Tuple2<Integer,Integer>> JKSB_Result_CGroup(
            DataStream<Tuple2<Integer,Integer>> grades,
            DataStream<Tuple2<Integer,Integer>> salaries,
            long windowSize) {
        DataStream<Tuple2<Integer,Integer>> apply = grades.coGroup(salaries)
                .where(new JKSB_Result_KeySelectorOne())
                .equalTo(new JKSB_Result_KeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>() {
                    Tuple2<Integer,Integer> tuple2=null;
                    @Override
                    public void coGroup(Iterable<Tuple2<Integer,Integer>> first, Iterable<Tuple2<Integer,Integer>> second, Collector<Tuple2<Integer,Integer>> collector) throws Exception {
                       tuple2 = new Tuple2<>();
                        for (Tuple2<Integer,Integer> s : first) {
                            tuple2.f0=s.f1;

                        }
                        for (Tuple2<Integer,Integer> s1 : second) {
                            tuple2.f1=s1.f1;
                        }
                        collector.collect(tuple2);
                    }
                });
        return apply;
    }
    private static class JKSB_Result_KeySelector implements KeySelector<Tuple2<Integer,Integer>, Integer> {
        @Override
        public Integer getKey(Tuple2<Integer,Integer> value) {
            return value.f0;
        }
    }
    private static class JKSB_Result_KeySelectorOne implements KeySelector<Tuple2<Integer,Integer>, Integer> {
        @Override
        public Integer getKey(Tuple2<Integer,Integer> value) {
            return value.f0+1;
        }
    }
}
