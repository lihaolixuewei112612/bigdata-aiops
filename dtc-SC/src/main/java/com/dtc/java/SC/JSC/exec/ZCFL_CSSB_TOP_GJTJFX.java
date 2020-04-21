package com.dtc.java.SC.JSC.exec;

import com.dtc.java.SC.JKZL.ExecutionEnvUtil;
import com.dtc.java.SC.JSC.source.JSC_CSSB_TOP_GJTJFX;
import com.dtc.java.SC.JSC.source.JSC_CSSB_TOP_GJTJFX_1;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Map;
import java.util.Properties;

/**
 * @Author : lihao
 * Created on : 2020-03-31
 * @Description : 驾驶舱监控大盘--厂商设备top告警统计分析
 */
public class ZCFL_CSSB_TOP_GJTJFX {
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
        DataStreamSource<Tuple5<String, String, Integer, Integer,Integer>> tuple4DataStreamSource = env.addSource(new JSC_CSSB_TOP_GJTJFX()).setParallelism(1);
        DataStreamSource<Tuple2<Integer,Integer>> tuple4DataStreamSource1 = env.addSource(new JSC_CSSB_TOP_GJTJFX_1()).setParallelism(1);
        tuple4DataStreamSource1.print();
//        DataStream<Tuple5<String, String, Integer, Integer, Integer>> tuple5DataStream = JKSB_Result_Join(tuple4DataStreamSource, tuple4DataStreamSource1, windowSizeMillis);
//        tuple5DataStream.filter(e->e.f0!=null).map(new JSC_CSSB_TOP_GJTJFX_Map()).print();

        env.execute("com.dtc.java.SC sart");
    }
    @Slf4j
    static class JSC_CSSB_TOP_GJTJFX_Map implements MapFunction<Tuple5<String, String, Integer, Integer, Integer>, Tuple5<String,String,Integer, Integer,Double>> {
        @Override
        public  Tuple5<String,String,Integer, Integer,Double> map(Tuple5<String, String, Integer, Integer, Integer> sourceEvent) {
           String id= sourceEvent.f0;
          String name = sourceEvent.f1;
            Integer  Num_GJ= sourceEvent.f2;
            Integer Num_ALL = sourceEvent.f3;
            Integer ALL_NUMBER= sourceEvent.f4;
            double result =Double.parseDouble(String.valueOf(Num_GJ))/Double.parseDouble(String.valueOf(ALL_NUMBER));
            double v2 = Double.parseDouble(String.format("%.3f", result));
            return Tuple5.of(id,name,Num_GJ,Num_ALL,v2);
        }
    }

    private static DataStream<Tuple5<String, String, Integer, Integer,Integer>> JKSB_Result_Join(
            DataStream<Tuple5<String, String, Integer, Integer,Integer>> grades,
            DataStream<Tuple2<Integer,Integer>> salaries,
            long windowSize) {
        DataStream<Tuple5<String, String, Integer, Integer,Integer>> apply = grades.join(salaries)
                .where(new TOp_Result_KeySelectorOne())
                .equalTo(new TOP_Result_KeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new JoinFunction<Tuple5<String, String, Integer, Integer, Integer>, Tuple2<Integer, Integer>,Tuple5<String, String, Integer, Integer,Integer>>() {
                    @Override
                    public Tuple5<String, String, Integer, Integer, Integer> join(Tuple5<String, String, Integer, Integer, Integer> first, Tuple2<Integer, Integer> second) throws Exception {
                        return Tuple5.of(first.f0,first.f1,first.f2,first.f3,second.f0);
                    }
                });
        return apply;
    }
    private static class TOP_Result_KeySelector implements KeySelector<Tuple2<Integer,Integer>, Integer> {
        @Override
        public Integer getKey(Tuple2<Integer,Integer> value) {
            return value.f1;
        }
    }
    private static class TOp_Result_KeySelectorOne implements KeySelector<Tuple5<String, String, Integer, Integer,Integer>, Integer> {
        @Override
        public Integer getKey(Tuple5<String, String, Integer, Integer,Integer> value) {
            return value.f4;
        }
    }
}
