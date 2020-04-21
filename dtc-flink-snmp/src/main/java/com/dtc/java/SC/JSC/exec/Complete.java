package com.dtc.java.SC.JSC.exec;

import com.dtc.java.SC.JKZL.ExecutionEnvUtil;
import com.dtc.java.SC.JSC.model.ModelFirst;
import com.dtc.java.SC.JSC.model.ModelSecond;
import com.dtc.java.SC.JSC.model.ModelThree;
import com.dtc.java.SC.JSC.sink.MysqlSinkJSC;
import com.dtc.java.SC.JSC.sink.MysqlSinkJSC_TOP;
import com.dtc.java.SC.JSC.sink.MysqlSinkJSC_YC;
import com.dtc.java.SC.JSC.source.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @Author : lihao
 * Created on : 2020-03-31
 * @Description : 驾驶舱监控大盘--今日告警总类
 */
public class Complete {
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
        //今日告警
        DataStreamSource<Tuple2<String, Integer>> DaPingWCLAlarm = env.addSource(new JSC_Alarm_level()).setParallelism(1);
        DataStream<ModelThree> ycsb_lb_modelSplitStream = getYcsb_lb_modelSplitStream(DaPingWCLAlarm, windowSizeMillis);
        //未关闭告警
        DataStreamSource<Tuple2<Integer, Integer>> integerDataStreamSource = env.addSource(new JSC_WGBGJ()).setParallelism(1);
        //（一般，严重，较严重，灾难，总告警数，未关闭告警,标志位）
        DataStream<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> tuple6DataStream = YCLB_Finally_CGroup111(ycsb_lb_modelSplitStream, integerDataStreamSource, windowSizeMillis).filter(e -> e.f0 != null);
        //监控设备
        DataStreamSource<Tuple2<Integer, Integer>> tuple2DataStreamSource = env.addSource(new JSC_AllNum()).setParallelism(1);
        DataStreamSource<Tuple2<Integer, Integer>> tuple2DataStreamSource1 = env.addSource(new JSC_ZCAllNum()).setParallelism(1);
        DataStream<Tuple2<Integer, Integer>> tuple2DataStream = JKSB_Result_CGroup(tuple2DataStreamSource, tuple2DataStreamSource1, windowSizeMillis);
        //(总设备数，正常，异常，标志位)
        SingleOutputStreamOperator<Tuple4<Integer, Integer, Integer, Integer>> JKSB_Map = tuple2DataStream.map(new JKSB_MyMapFunctionV());
        //（一般，严重，较严重，灾难，总告警数，未关闭告警,总设备数，正常，异常）写入msyql中
        DataStream<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> tuple9DataStream = YCLB_Finally_CGroup222(tuple6DataStream, JKSB_Map, windowSizeMillis).filter(e -> e.f0 != null);
        tuple9DataStream.addSink(new MysqlSinkJSC(properties));


        //资产类型告警统计
        DataStreamSource<Tuple2<String, Integer>> JSC_ZCGJTJ_YC = env.addSource(new JSC_ZCGJTJ_YC()).setParallelism(1);
        DataStreamSource<Tuple2<String, Integer>> JSC_ZCGJTJ_ALL = env.addSource(new JSC_ZCGJTJ_ALL()).setParallelism(1);
        DataStream<Tuple3<String, Integer, Integer>> ZCLXGJTJ_Stream = ZCLXGJTJ_Result_CGroup(JSC_ZCGJTJ_YC, JSC_ZCGJTJ_ALL, windowSizeMillis);
        //（名称，异常数，总数，比值）
        SingleOutputStreamOperator<Tuple4<String, Integer, Integer, Double>> map = ZCLXGJTJ_Stream.map(new JSC_ZCGJTJ_ALL_MAP());
//资产分类统计
        DataStreamSource<Tuple2<String, Integer>> JSC_ZCGJTJ_ALL_Stream = env.addSource(new JSC_ZCGJTJ_ALL()).setParallelism(1);
        DataStreamSource<Tuple2<String, Integer>> JSC_ZC_Used_Num_Stream = env.addSource(new JSC_ZC_Used_Num()).setParallelism(1);
        DataStreamSource<Tuple2<String, Integer>> JSC_ZCGJTJ_YC_Online_Stream = env.addSource(new JSC_ZCGJTJ_YC_Online()).setParallelism(1);
        DataStream<Tuple3<String, Integer, Integer>> JSC_ZCGJTJ_Stream = ZCFLTJ_Result_CGroup(JSC_ZCGJTJ_ALL_Stream, JSC_ZC_Used_Num_Stream, windowSizeMillis);
        DataStream<Tuple4<String, Integer, Integer, Integer>> tuple4DataStream = ZCFLTJ_Finally_CGroup(JSC_ZCGJTJ_Stream, JSC_ZCGJTJ_YC_Online_Stream, windowSizeMillis);
        //（名称，异常数，总数，比值）
        //（名称，总数，已使用，异常数据，异常比）
        SingleOutputStreamOperator<Tuple5<String, Integer, Integer, Integer, Double>> map1 = tuple4DataStream.filter(e -> e.f0 != null).map(new ZCFLTJ_MapFunctionV());
        DataStream<Tuple8<String, Integer, Integer, Double, Integer, Integer, Integer, Double>> tuple8DataStream = YCLB_Finally_CGroup333(map, map1, windowSizeMillis).filter(e -> e.f0 != null).filter(e -> e.f5 != null);
        tuple8DataStream.addSink(new MysqlSinkJSC_YC(properties));
//厂商设备top告警统计分析
       DataStreamSource<Tuple5<String, String, Integer, Integer,Integer>> tuple4DataStreamSource = env.addSource(new JSC_CSSB_TOP_GJTJFX()).setParallelism(1);
        DataStreamSource<Tuple2<Integer,Integer>> tuple4DataStreamSource1 = env.addSource(new JSC_CSSB_TOP_GJTJFX_1()).setParallelism(1);
        DataStream<Tuple5<String, String, Integer, Integer, Integer>> tuple5DataStream = JKSB_Result_Join(tuple4DataStreamSource, tuple4DataStreamSource1, windowSizeMillis);
        SingleOutputStreamOperator<Tuple5<String, String, Integer, Integer, Double>> map2 = tuple5DataStream.filter(e -> e.f0 != null).map(new TOp_CSSB_TOP_GJTJFX_Map());
        map2.addSink(new MysqlSinkJSC_TOP(properties));

        env.execute("com.dtc.java.SC sart");
    }

    private static DataStream<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> YCLB_Finally_CGroup111(
            DataStream<ModelThree> grades,
            DataStream<Tuple2<Integer, Integer>> salaries,
            long windowSize) {
        DataStream<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> apply = grades.coGroup(salaries)
                .where(new First_one())
                .equalTo(new First_two())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<ModelThree, Tuple2<Integer, Integer>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple7 = null;

                    @Override
                    public void coGroup(Iterable<ModelThree> first, Iterable<Tuple2<Integer, Integer>> second, Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> collector) throws Exception {
                        tuple7 = new Tuple7<>();
                        for (ModelThree s : first) {
                            tuple7.f0 = s.getLevel_one();
                            tuple7.f1 = s.getLevel_two();
                            tuple7.f2 = s.getLevel_three();
                            tuple7.f3 = s.getLevel_four();
                            tuple7.f4 = s.getZN();
                        }
                        for (Tuple2<Integer, Integer> s : second) {
                            tuple7.f5 = s.f1;
                            tuple7.f6 = 1;
                        }
                        collector.collect(tuple7);
                    }
                });
        return apply;
    }

    private static class First_one implements KeySelector<ModelThree, Integer> {
        @Override
        public Integer getKey(ModelThree value) {
            return value.getFlag();
        }
    }

    private static class First_two implements KeySelector<Tuple2<Integer, Integer>, Integer> {
        @Override
        public Integer getKey(Tuple2<Integer, Integer> value) {
            return value.f0;
        }
    }

    private static DataStream<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> YCLB_Finally_CGroup222(
            DataStream<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> grades,
            DataStream<Tuple4<Integer, Integer, Integer, Integer>> salaries,
            long windowSize) {
        DataStream<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> apply = grades.coGroup(salaries)
                .where(new First_three())
                .equalTo(new First_four())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple4<Integer, Integer, Integer, Integer>, Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple9 = null;

                    @Override
                    public void coGroup(Iterable<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> first, Iterable<Tuple4<Integer, Integer, Integer, Integer>> second, Collector<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> collector) throws Exception {
                        tuple9 = new Tuple9<>();
                        for (Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> s : first) {
                            //（一般，严重，较严重，灾难，总告警数，未关闭告警,标志位）
                            tuple9.f0 = s.f0;
                            tuple9.f1 = s.f1;
                            tuple9.f2 = s.f2;
                            tuple9.f3 = s.f3;
                            tuple9.f4 = s.f4;
                            tuple9.f5 = s.f5;
                        }
                        for (Tuple4<Integer, Integer, Integer, Integer> s : second) {
                            tuple9.f6 = s.f0;
                            tuple9.f7 = s.f1;
                            tuple9.f8 = s.f2;
                        }
                        collector.collect(tuple9);
                    }
                });
        return apply;
    }

    private static class First_three implements KeySelector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Integer> {
        @Override
        public Integer getKey(Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> value) {
            return value.f6;
        }
    }

    private static class First_four implements KeySelector<Tuple4<Integer, Integer, Integer, Integer>, Integer> {
        @Override
        public Integer getKey(Tuple4<Integer, Integer, Integer, Integer> value) {
            return value.f3;
        }
    }


    private static DataStream<Tuple8<String, Integer, Integer, Double, Integer, Integer, Integer, Double>> YCLB_Finally_CGroup333(
            //（名称，异常数，总数，比值）
            DataStream<Tuple4<String, Integer, Integer, Double>> grades,
            //（名称，总数，已使用，异常数据，异常比）
            DataStream<Tuple5<String, Integer, Integer, Integer, Double>> salaries,
            long windowSize) {
        DataStream<Tuple8<String, Integer, Integer, Double, Integer, Integer, Integer, Double>> apply = grades.coGroup(salaries)
                .where(new First_five())
                .equalTo(new First_six())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<Tuple4<String, Integer, Integer, Double>, Tuple5<String, Integer, Integer, Integer, Double>, Tuple8<String, Integer, Integer, Double, Integer, Integer, Integer, Double>>() {
                    Tuple8<String, Integer, Integer, Double, Integer, Integer, Integer, Double> tuple8 = null;

                    @Override
                    public void coGroup(Iterable<Tuple4<String, Integer, Integer, Double>> first, Iterable<Tuple5<String, Integer, Integer, Integer, Double>> second, Collector<Tuple8<String, Integer, Integer, Double, Integer, Integer, Integer, Double>> collector) throws Exception {
                        tuple8 = new Tuple8<>();
                        for (Tuple4<String, Integer, Integer, Double> s : first) {
                            //（一般，严重，较严重，灾难，总告警数，未关闭告警,标志位）
                            tuple8.f0 = s.f0;
                            tuple8.f1 = s.f1;
                            tuple8.f2 = s.f2;
                            tuple8.f3 = s.f3;
                        }
                        for (Tuple5<String, Integer, Integer, Integer, Double> s : second) {
                            tuple8.f4 = s.f1;
                            tuple8.f5 = s.f2;
                            tuple8.f6 = s.f3;
                            tuple8.f7 = s.f4;
                        }
                        collector.collect(tuple8);
                    }
                });
        return apply;
    }

    private static class First_five implements KeySelector<Tuple4<String, Integer, Integer, Double>, String> {
        @Override
        public String getKey(Tuple4<String, Integer, Integer, Double> value) {
            return value.f0;
        }
    }

    private static class First_six implements KeySelector<Tuple5<String, Integer, Integer, Integer, Double>, String> {
        @Override
        public String getKey(Tuple5<String, Integer, Integer, Integer, Double> value) {
            return value.f0;
        }
    }

    @Slf4j
    static class JSC_CSSB_TOP_GJTJFX_Map implements MapFunction<Tuple4<String, String, Integer, Integer>, Tuple5<String, String, Integer, Integer, Double>> {
        @Override
        public Tuple5<String, String, Integer, Integer, Double> map(Tuple4<String, String, Integer, Integer> sourceEvent) {
            String id = sourceEvent.f0;
            String name = sourceEvent.f1;
            Integer Num_GJ = sourceEvent.f2;
            Integer Num_ALL = sourceEvent.f3;
            double result = Double.parseDouble(String.valueOf(Num_GJ)) / Double.parseDouble(String.valueOf(Num_ALL));
            double v2 = Double.parseDouble(String.format("%.3f", result));
            return Tuple5.of(id, name, Num_GJ, Num_ALL, v2);
        }
    }

    private static DataStream<ModelThree> getYcsb_lb_modelSplitStream(DataStreamSource<Tuple2<String, Integer>> sum, long windowSize) {
        SplitStream<Tuple2<String, Integer>> split = sum.split((OutputSelector<Tuple2<String, Integer>>) event -> {
            List<String> output = new ArrayList<>();
            String type = event.f0;
            if ("1".equals(type)) {
                output.add("level_1");
            } else if ("2".equals(type)) {
                output.add("level_2");
            } else if ("3".equals(type)) {
                output.add("level_3");
            } else if ("4".equals(type)) {
                output.add("level_4");
            }
            return output;
        });
        DataStream<Tuple2<String, Integer>> select_1 = split.select("level_1");
        DataStream<Tuple2<String, Integer>> select_2 = split.select("level_2");
        DataStream<Tuple2<String, Integer>> select_3 = split.select("level_3");
        DataStream<Tuple2<String, Integer>> select_4 = split.select("level_4");
        DataStream<ModelFirst> ycsb_lb_result_modelDataStream = YCLB_Result_CGroup(select_1, select_2, windowSize);
        DataStream<ModelFirst> ycsb_lb_result_modelDataStream1 = YCLB_Result_CGroup(select_3, select_4, windowSize);
        DataStream<ModelSecond> modelSecondDataStream = YCLB_Finally_CGroup(ycsb_lb_result_modelDataStream, ycsb_lb_result_modelDataStream1, windowSize);
        SingleOutputStreamOperator<ModelThree> map = modelSecondDataStream.map(new MyMapFunctionV3());
        return map;
    }

    @Slf4j
    static class MyMapFunctionV3 implements MapFunction<ModelSecond, ModelThree> {
        @Override
        public ModelThree map(ModelSecond sourceEvent) {
            int level_one = sourceEvent.getLevel_one();
            int level_two = sourceEvent.getLevel_two();
            int level_three = sourceEvent.getLevel_three();
            int level_four = sourceEvent.getLevel_four();
            int ZN = level_one + level_two + level_three + level_four;

            return new ModelThree(level_one, level_two, level_three, level_four, ZN, 1);
        }
    }

    private static DataStream<ModelFirst> YCLB_Result_CGroup(
            DataStream<Tuple2<String, Integer>> grades,
            DataStream<Tuple2<String, Integer>> salaries,
            long windowSize) {
        DataStream<ModelFirst> apply = grades.coGroup(salaries)
                .where(new YCFB_Result_KeySelectorOne())
                .equalTo(new YCFB_Result_KeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, ModelFirst>() {
                    ModelFirst mf = null;

                    @Override
                    public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<ModelFirst> collector) throws Exception {
                        mf = new ModelFirst();
                        for (Tuple2<String, Integer> s : first) {
                            mf.setLevel_1(s.f0);
                            mf.setLevel_one(s.f1);
                        }
                        for (Tuple2<String, Integer> s1 : second) {
                            mf.setLevel_2(s1.f0);
                            mf.setLevel_two(s1.f1);
                        }
                        collector.collect(mf);
                    }
                });
        return apply;
    }

    private static DataStream<ModelSecond> YCLB_Finally_CGroup(
            DataStream<ModelFirst> grades,
            DataStream<ModelFirst> salaries,
            long windowSize) {
        DataStream<ModelSecond> apply = grades.coGroup(salaries)
                .where(new YCFB_Finall_KeySelector())
                .equalTo(new YCFB_Finall_KeySelectorOne())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<ModelFirst, ModelFirst, ModelSecond>() {
                    ModelSecond ms = null;

                    @Override
                    public void coGroup(Iterable<ModelFirst> first, Iterable<ModelFirst> second, Collector<ModelSecond> collector) throws Exception {
                        ms = new ModelSecond();
                        for (ModelFirst s : first) {
                            ms.setLevel_1(s.getLevel_1());
                            ms.setLevel_one(s.getLevel_one());
                            ms.setLevel_2(s.getLevel_2());
                            ms.setLevel_two(s.getLevel_two());
                        }
                        for (ModelFirst s : second) {
                            ms.setLevel_3(s.getLevel_1());
                            ms.setLevel_three(s.getLevel_one());
                            ms.setLevel_4(s.getLevel_2());
                            ms.setLevel_four(s.getLevel_two());
                        }
                        collector.collect(ms);
                    }
                });
        return apply;
    }


    private static class YCFB_Result_KeySelector implements KeySelector<Tuple2<String, Integer>, Integer> {
        @Override
        public Integer getKey(Tuple2<String, Integer> value) {
            return Integer.parseInt(value.f0);
        }
    }

    private static class YCFB_Result_KeySelectorOne implements KeySelector<Tuple2<String, Integer>, Integer> {
        @Override
        public Integer getKey(Tuple2<String, Integer> value) {
            return Integer.parseInt(value.f0) + 1;
        }
    }

    private static class YCFB_Finall_KeySelector implements KeySelector<ModelFirst, Integer> {
        @Override
        public Integer getKey(ModelFirst value) {
            return Integer.parseInt(value.getLevel_1()) + 2;
        }
    }

    private static class YCFB_Finall_KeySelectorOne implements KeySelector<ModelFirst, Integer> {
        @Override
        public Integer getKey(ModelFirst value) {
            return Integer.parseInt(value.getLevel_1());
        }
    }

    @Slf4j
    static class JKSB_MyMapFunctionV implements MapFunction<Tuple2<Integer, Integer>, Tuple4<Integer, Integer, Integer, Integer>> {
        @Override
        public Tuple4<Integer, Integer, Integer, Integer> map(Tuple2<Integer, Integer> sourceEvent) {
            Integer All_Num = sourceEvent.f0;
            Integer ZC_Num = sourceEvent.f1;
            int BZC_Num = All_Num - ZC_Num;

            return Tuple4.of(All_Num, ZC_Num, BZC_Num, 1);
        }
    }

    private static DataStream<Tuple2<Integer, Integer>> JKSB_Result_CGroup(
            DataStream<Tuple2<Integer, Integer>> grades,
            DataStream<Tuple2<Integer, Integer>> salaries,
            long windowSize) {
        DataStream<Tuple2<Integer, Integer>> apply = grades.coGroup(salaries)
                .where(new JKSB_Result_KeySelectorOne())
                .equalTo(new JKSB_Result_KeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    Tuple2<Integer, Integer> tuple2 = null;

                    @Override
                    public void coGroup(Iterable<Tuple2<Integer, Integer>> first, Iterable<Tuple2<Integer, Integer>> second, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
                        tuple2 = new Tuple2<>();
                        for (Tuple2<Integer, Integer> s : first) {
                            tuple2.f0 = s.f1;

                        }
                        for (Tuple2<Integer, Integer> s1 : second) {
                            tuple2.f1 = s1.f1;
                        }
                        collector.collect(tuple2);
                    }
                });
        return apply;
    }

    @Slf4j
    static class ZCFLTJ_MapFunctionV implements MapFunction<Tuple4<String, Integer, Integer, Integer>, Tuple5<String, Integer, Integer, Integer, Double>> {
        @Override
        public Tuple5<String, Integer, Integer, Integer, Double> map(Tuple4<String, Integer, Integer, Integer> sourceEvent) {
            String name = sourceEvent.f0;
            Integer All_Num = sourceEvent.f1;
            Integer Used_Num = sourceEvent.f2;
            Integer YC_Num = sourceEvent.f3;
            double result = Double.parseDouble(String.valueOf(YC_Num)) / Double.parseDouble(String.valueOf(All_Num));
            double v2 = Double.parseDouble(String.format("%.3f", result));

            return Tuple5.of(name, All_Num, Used_Num, YC_Num, v2);
        }
    }

    private static DataStream<Tuple3<String, Integer, Integer>> ZCFLTJ_Result_CGroup(
            DataStream<Tuple2<String, Integer>> grades,
            DataStream<Tuple2<String, Integer>> salaries,
            long windowSize) {
        DataStream<Tuple3<String, Integer, Integer>> apply = grades.coGroup(salaries)
                .where(new ZCFLTJ_Result_KeySelector2())
                .equalTo(new ZCFLTJ_Result_KeySelector2())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
                    Tuple3<String, Integer, Integer> tuple3 = null;

                    @Override
                    public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<Tuple3<String, Integer, Integer>> collector) throws Exception {
                        tuple3 = new Tuple3<>();
                        for (Tuple2<String, Integer> s : first) {
                            tuple3.f0 = s.f0;
                            tuple3.f1 = s.f1;

                        }
                        for (Tuple2<String, Integer> s1 : second) {
                            tuple3.f2 = s1.f1;
                        }
                        collector.collect(tuple3);
                    }
                });
        return apply;
    }

    private static DataStream<Tuple4<String, Integer, Integer, Integer>> ZCFLTJ_Finally_CGroup(
            DataStream<Tuple3<String, Integer, Integer>> grades,
            DataStream<Tuple2<String, Integer>> salaries,
            long windowSize) {
        DataStream<Tuple4<String, Integer, Integer, Integer>> apply = grades.coGroup(salaries)
                .where(new ZCFLTJ_Result_KeySelector())
                .equalTo(new ZCFLTJ_Result_KeySelector1())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>, Tuple4<String, Integer, Integer, Integer>>() {
                    Tuple4<String, Integer, Integer, Integer> tuple4 = null;

                    @Override
                    public void coGroup(Iterable<Tuple3<String, Integer, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<Tuple4<String, Integer, Integer, Integer>> collector) throws Exception {
                        tuple4 = new Tuple4<>();
                        for (Tuple3<String, Integer, Integer> s : first) {
                            tuple4.f0 = s.f0;
                            tuple4.f1 = s.f1;
                            tuple4.f2 = s.f2;
                        }
                        for (Tuple2<String, Integer> s1 : second) {
                            tuple4.f3 = s1.f1;
                        }
                        collector.collect(tuple4);
                    }
                });
        return apply;
    }

    private static class ZCFLTJ_Result_KeySelector1 implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> value) {
            return value.f0;
        }
    }

    private static class ZCFLTJ_Result_KeySelector implements KeySelector<Tuple3<String, Integer, Integer>, String> {
        @Override
        public String getKey(Tuple3<String, Integer, Integer> value) {
            return value.f0;
        }
    }

    private static class ZCFLTJ_Result_KeySelector2 implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> value) {
            return value.f0;
        }
    }

    @Slf4j
    static class JSC_ZCGJTJ_ALL_MAP implements MapFunction<Tuple3<String, Integer, Integer>, Tuple4<String, Integer, Integer, Double>> {
        @Override
        public Tuple4<String, Integer, Integer, Double> map(Tuple3<String, Integer, Integer> sourceEvent) {
            String name = sourceEvent.f0;
            Integer YC_Num = sourceEvent.f1;
            Integer All_Num = sourceEvent.f2;
            double result = Double.parseDouble(String.valueOf(YC_Num)) / Double.parseDouble(String.valueOf(All_Num));
            double v2 = Double.parseDouble(String.format("%.3f", result));

            return Tuple4.of(name, YC_Num, All_Num, v2);
        }
    }

    private static DataStream<Tuple3<String, Integer, Integer>> ZCLXGJTJ_Result_CGroup(
            DataStream<Tuple2<String, Integer>> grades,
            DataStream<Tuple2<String, Integer>> salaries,
            long windowSize) {
        DataStream<Tuple3<String, Integer, Integer>> apply = grades.coGroup(salaries)
                .where(new ZCLXGJTJ_Result_KeySelector())
                .equalTo(new ZCLXGJTJ_Result_KeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
                    Tuple3<String, Integer, Integer> tuple3 = null;

                    @Override
                    public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<Tuple3<String, Integer, Integer>> collector) throws Exception {
                        tuple3 = new Tuple3<>();
                        for (Tuple2<String, Integer> s : first) {
                            tuple3.f0 = s.f0;
                            tuple3.f1 = s.f1;

                        }
                        for (Tuple2<String, Integer> s1 : second) {
                            tuple3.f2 = s1.f1;
                        }
                        collector.collect(tuple3);
                    }
                });
        return apply;
    }

    private static class ZCLXGJTJ_Result_KeySelector implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> value) {
            return value.f0;
        }
    }

    private static class JKSB_Result_KeySelector implements KeySelector<Tuple2<Integer, Integer>, Integer> {
        @Override
        public Integer getKey(Tuple2<Integer, Integer> value) {
            return value.f0;
        }
    }

    private static class JKSB_Result_KeySelectorOne implements KeySelector<Tuple2<Integer, Integer>, Integer> {
        @Override
        public Integer getKey(Tuple2<Integer, Integer> value) {
            return value.f0 + 1;
        }
    }
    @Slf4j
    static class TOp_CSSB_TOP_GJTJFX_Map implements MapFunction<Tuple5<String, String, Integer, Integer, Integer>, Tuple5<String,String,Integer, Integer,Double>> {
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
