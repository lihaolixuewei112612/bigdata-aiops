package com.dtc.java.SC.JSC.exec;//package com.dtc.java.shucang.JaShiCang.exec;
//
//import com.dtc.java.shucang.JFSBWGBGJ.ExecutionEnvUtil;
//import com.dtc.java.shucang.JaShiCang.model.ModelFirst;
//import com.dtc.java.shucang.JaShiCang.model.ModelSecond;
//import com.dtc.java.shucang.JaShiCang.model.ModelThree;
//import com.dtc.java.shucang.JaShiCang.source.JSC_Alarm_level;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.api.common.functions.CoGroupFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.collector.selector.OutputSelector;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.datastream.SplitStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.util.Collector;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//
///**
// * @Author : lihao
// * Created on : 2020-03-31
// * @Description : 驾驶舱监控大盘--今日告警
// */
//public class Alarm_level_Main {
//    public static void main(String[] args) throws Exception {
//
//        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
//        Map<String, String> stringStringMap = parameterTool.toMap();
//        Properties properties = new Properties();
//        for (String key : stringStringMap.keySet()) {
//            if (key.startsWith("mysql")) {
//                properties.setProperty(key, stringStringMap.get(key));
//            }
//        }
//        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
//        int windowSizeMillis = 6000;
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        /**各机房各区域各机柜设备总数*/
//        DataStreamSource<Tuple2<String,Integer>> DaPingWCLAlarm = env.addSource(new JSC_Alarm_level()).setParallelism(1);
//        DataStream<ModelThree> ycsb_lb_modelSplitStream = getYcsb_lb_modelSplitStream(DaPingWCLAlarm, windowSizeMillis);
//        ycsb_lb_modelSplitStream.print();
//        env.execute("com.dtc.java.SC sart");
//    }
//    private static DataStream<ModelThree> getYcsb_lb_modelSplitStream(DataStreamSource<Tuple2<String,Integer>> sum, long windowSize) {
//        SplitStream<Tuple2<String,Integer>> split = sum.split((OutputSelector<Tuple2<String,Integer>>) event -> {
//            List<String> output = new ArrayList<>();
//            String type = event.f0;
//            if ("1".equals(type)) {
//                output.add("level_1");
//            } else if ("2".equals(type)) {
//                output.add("level_2");
//            } else if ("3".equals(type)) {
//                output.add("level_3");
//            } else if ("4".equals(type)) {
//                output.add("level_4");
//            }
//            return output;
//        });
//        DataStream<Tuple2<String,Integer>> select_1 = split.select("level_1");
//        DataStream<Tuple2<String,Integer>> select_2 = split.select("level_2");
//        DataStream<Tuple2<String,Integer>> select_3 = split.select("level_3");
//        DataStream<Tuple2<String,Integer>> select_4 = split.select("level_4");
//        DataStream<ModelFirst> ycsb_lb_result_modelDataStream = YCLB_Result_CGroup(select_1, select_2, windowSize);
//        DataStream<ModelFirst> ycsb_lb_result_modelDataStream1 = YCLB_Result_CGroup(select_3, select_4, windowSize);
//        DataStream<ModelSecond> modelSecondDataStream = YCLB_Finally_CGroup(ycsb_lb_result_modelDataStream, ycsb_lb_result_modelDataStream1, windowSize);
//        SingleOutputStreamOperator<ModelThree> map = modelSecondDataStream.map(new MyMapFunctionV3());
//        return map;
//    }
//    @Slf4j
//    static class MyMapFunctionV3 implements MapFunction<ModelSecond, ModelThree> {
//        @Override
//        public ModelThree map(ModelSecond sourceEvent) {
//            int level_one = sourceEvent.getLevel_one();
//            int level_two = sourceEvent.getLevel_two();
//            int level_three = sourceEvent.getLevel_three();
//            int level_four = sourceEvent.getLevel_four();
//            int ZN = level_one+level_two+level_three+level_four;
//
//            return new ModelThree(level_one,level_two,level_three,level_four,ZN);
//        }
//    }
//    private static DataStream<ModelFirst> YCLB_Result_CGroup(
//            DataStream<Tuple2<String,Integer>> grades,
//            DataStream<Tuple2<String,Integer>> salaries,
//            long windowSize) {
//        DataStream<ModelFirst> apply = grades.coGroup(salaries)
//                .where(new YCFB_Result_KeySelectorOne())
//                .equalTo(new YCFB_Result_KeySelector())
//                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
//                .apply(new CoGroupFunction<Tuple2<String,Integer>, Tuple2<String,Integer>,ModelFirst>() {
//                    ModelFirst mf = null;
//
//                    @Override
//                    public void coGroup(Iterable<Tuple2<String,Integer>> first, Iterable<Tuple2<String,Integer>> second, Collector<ModelFirst> collector) throws Exception {
//                      mf = new ModelFirst();
//                        for (Tuple2<String,Integer> s : first) {
//                           mf.setLevel_1(s.f0);
//                           mf.setLevel_one(s.f1);
//                        }
//                        for (Tuple2<String,Integer> s1 : second) {
//                            mf.setLevel_2(s1.f0);
//                            mf.setLevel_two(s1.f1);
//                        }
//                        collector.collect(mf);
//                    }
//                });
//        return apply;
//    }
//    private static DataStream<ModelSecond> YCLB_Finally_CGroup(
//            DataStream<ModelFirst> grades,
//            DataStream<ModelFirst> salaries,
//            long windowSize) {
//        DataStream<ModelSecond> apply = grades.coGroup(salaries)
//                .where(new YCFB_Finall_KeySelector())
//                .equalTo(new YCFB_Finall_KeySelectorOne())
//                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
//                .apply(new CoGroupFunction<ModelFirst, ModelFirst, ModelSecond>() {
//                    ModelSecond ms = null;
//
//                    @Override
//                    public void coGroup(Iterable<ModelFirst> first, Iterable<ModelFirst> second, Collector<ModelSecond> collector) throws Exception {
//                        ms = new ModelSecond();
//                        for (ModelFirst s : first) {
//                            ms.setLevel_1(s.getLevel_1());
//                            ms.setLevel_one(s.getLevel_one());
//                            ms.setLevel_2(s.getLevel_2());
//                            ms.setLevel_two(s.getLevel_two());
//                        }
//                        for (ModelFirst s : second) {
//                            ms.setLevel_3(s.getLevel_1());
//                            ms.setLevel_three(s.getLevel_one());
//                            ms.setLevel_4(s.getLevel_2());
//                            ms.setLevel_four(s.getLevel_two());
//                        }
//                        collector.collect(ms);
//                    }
//                });
//        return apply;
//    }
//
//
//    private static class YCFB_Result_KeySelector implements KeySelector<Tuple2<String,Integer>, Integer> {
//        @Override
//        public Integer getKey(Tuple2<String,Integer> value) {
//            return Integer.parseInt(value.f0);
//        }
//    }
//    private static class YCFB_Result_KeySelectorOne implements KeySelector<Tuple2<String,Integer>, Integer> {
//        @Override
//        public Integer getKey(Tuple2<String,Integer> value) {
//            return Integer.parseInt(value.f0)+1;
//        }
//    }
//    private static class YCFB_Finall_KeySelector implements KeySelector<ModelFirst, Integer> {
//        @Override
//        public Integer getKey(ModelFirst value) {
//            return Integer.parseInt(value.getLevel_1())+2;
//        }
//    }
//    private static class YCFB_Finall_KeySelectorOne implements KeySelector<ModelFirst, Integer> {
//        @Override
//        public Integer getKey(ModelFirst value) {
//            return Integer.parseInt(value.getLevel_1());
//        }
//    }
//}
