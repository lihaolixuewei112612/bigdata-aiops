package com.dtc.java.SC.JSC.exec;

import com.dtc.java.SC.JKZL.ExecutionEnvUtil;
import com.dtc.java.SC.JSC.source.JSC_ZCGJTJ_ALL;
import com.dtc.java.SC.JSC.source.JSC_ZCGJTJ_YC;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
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
 * @Description : 驾驶舱监控大盘--资产类型告警统计
 */
public class ZCLXGJTJ_Main {
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
        DataStreamSource<Tuple2<String, Integer>> JSC_ZCGJTJ_YC = env.addSource(new JSC_ZCGJTJ_YC()).setParallelism(1);
        DataStreamSource<Tuple2<String, Integer>> JSC_ZCGJTJ_ALL = env.addSource(new JSC_ZCGJTJ_ALL()).setParallelism(1);
        DataStream<Tuple3<String, Integer, Integer>> tuple3DataStream = ZCLXGJTJ_Result_CGroup(JSC_ZCGJTJ_YC, JSC_ZCGJTJ_ALL, windowSizeMillis);
        tuple3DataStream.map(new JSC_ZCGJTJ_ALL_MAP()).print();

        env.execute("com.dtc.java.SC sart");
    }
    @Slf4j
    static class JSC_ZCGJTJ_ALL_MAP implements MapFunction<Tuple3<String, Integer, Integer>, Tuple4<String,Integer, Integer,Double>> {
        @Override
        public  Tuple4<String,Integer, Integer,Double> map(Tuple3<String, Integer, Integer> sourceEvent) {
           String name = sourceEvent.f0;
           Integer YC_Num = sourceEvent.f1;
            Integer All_Num = sourceEvent.f2;
            double result =Double.parseDouble(String.valueOf(YC_Num))/Double.parseDouble(String.valueOf(All_Num));
            double v2 = Double.parseDouble(String.format("%.3f", result));

            return Tuple4.of(name,YC_Num,All_Num,v2);
        }
    }
    private static DataStream<Tuple3<String,Integer,Integer>> ZCLXGJTJ_Result_CGroup(
            DataStream<Tuple2<String,Integer>> grades,
            DataStream<Tuple2<String,Integer>> salaries,
            long windowSize) {
        DataStream<Tuple3<String,Integer,Integer>> apply = grades.coGroup(salaries)
                .where(new ZCLXGJTJ_Result_KeySelector())
                .equalTo(new ZCLXGJTJ_Result_KeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<Tuple2<String,Integer>, Tuple2<String,Integer>,Tuple3<String,Integer,Integer>>() {
                    Tuple3<String,Integer,Integer> tuple3=null;
                    @Override
                    public void coGroup(Iterable<Tuple2<String,Integer>> first, Iterable<Tuple2<String,Integer>> second, Collector<Tuple3<String,Integer,Integer>> collector) throws Exception {
                       tuple3 = new Tuple3<>();
                        for (Tuple2<String,Integer> s : first) {
                            tuple3.f0=s.f0;
                            tuple3.f1=s.f1;

                        }
                        for (Tuple2<String,Integer> s1 : second) {
                            tuple3.f2=s1.f1;
                        }
                        collector.collect(tuple3);
                    }
                });
        return apply;
    }
    private static class ZCLXGJTJ_Result_KeySelector implements KeySelector<Tuple2<String,Integer>, String> {
        @Override
        public String getKey(Tuple2<String,Integer> value) {
            return value.f0;
        }
    }
}
