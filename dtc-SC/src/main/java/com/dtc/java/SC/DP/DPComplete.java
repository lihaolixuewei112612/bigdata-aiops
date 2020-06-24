package com.dtc.java.SC.DP;

import com.dtc.java.SC.DP.sink.MysqlSink_DP;
import com.dtc.java.SC.DP.sink.MysqlSink_DP_30D;
import com.dtc.java.SC.DP.sink.MysqlSink_DP_ZCDP;
import com.dtc.java.SC.DP.source.*;
import com.dtc.java.SC.JKZL.ExecutionEnvUtil;
import com.dtc.java.SC.JSC.model.ModelFirst;
import com.dtc.java.SC.JSC.sink.JSC_GL_SINK;
import com.dtc.java.SC.JSC.sink.JSC_GL_SINK_1;
import com.dtc.java.SC.JSC.sink.JSC_GL_SINK_2;
import com.dtc.java.SC.JSC.source.JSC_GL_SOURCE;
import com.dtc.java.SC.JSC.source.JSC_GL_SOURCE_1;
import com.dtc.java.SC.JSC.source.JSC_GL_SOURCE_2;
import com.dtc.java.SC.common.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

import static com.dtc.java.SC.DP.JSCComplete.JSC_EXEC;


/**
 * @Author : lihao
 * Created on : 2020-03-24
 * @Description : 数仓监控大盘指标总类
 */
public class DPComplete {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.getConfig().setGlobalJobParameters(parameterTool);
        int windowSizeMillis = Integer.parseInt(parameterTool.get(PropertiesConstants.INTERVAL_TIME));
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM, 2));
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
        //每隔50s进行启动一个检查点
        if (parameterTool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE, true)) {
            env.enableCheckpointing(parameterTool.getInt(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL, 60000)); // create a checkpoint every 5 seconds
        }
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        // 确保检查点之间有进行1s的进度
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        // 检查点必须在一分钟内完成，或者被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //开启checkpoints的外部持久化，但是在job失败的时候不会自动清理，需要自己手工清理state
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        /**各机房各区域各机柜设备总数*/
        //大盘今日监控设备数
        DP_EXEC(env, windowSizeMillis);
        JSC_EXEC(env, windowSizeMillis);
        //管理大盘
        env.addSource(new JSC_GL_SOURCE()).addSink(new JSC_GL_SINK());
        env.addSource(new JSC_GL_SOURCE_1()).addSink(new JSC_GL_SINK_1());
        env.addSource(new JSC_GL_SOURCE_2()).addSink(new JSC_GL_SINK_2());

////
////        //我的总览
//        env.addSource(new WdzlSource()).addSink(new WdzlSink());
        env.execute("dtc-数仓");
    }

    private static void DP_EXEC(StreamExecutionEnvironment env, int windowSizeMillis) {
        DataStreamSource<Tuple2<Integer,Integer>> zsStream = env.addSource(new DaPingAllNum()).setParallelism(1);
        //大盘今日告警数
        //DataStreamSource<Tuple2<Integer, Integer>> tuple2DataStreamSource = env.addSource(new DaPingAlarm()).setParallelism(1);
        //大盘今日工单
        DataStreamSource<Tuple2<Integer, Integer>> today_DP_WO = env.addSource(new DaPingOrder()).setParallelism(1);
        //变更
        DataStreamSource<Tuple2<Integer, Integer>> today_DP_BG = env.addSource(new DaPingBianGengOrder()).setParallelism(1);
        DataStream<Tuple3<Integer, Integer, Integer>> today_WO_BG = First_CGroup(today_DP_WO, today_DP_BG, windowSizeMillis);
        //正常运行 驾驶舱-监控大盘已经有了
//        DataStreamSource<Tuple2<Integer, Integer>> today_ZCSB = env.addSource(new DaPingZCAllNum()).setParallelism(1);
        //未处理告警数
        DataStreamSource<Tuple2<String,Integer>> DPWCLAlarm = env.addSource(new DaPingWCLAlarm()).setParallelism(1);
        DataStream<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> ycsb_lb_modelSplitStream = getYcsb_lb_modelSplitStream(DPWCLAlarm, windowSizeMillis);
        //(标志，工单，变更，等级1，2，3，4，总和)
        DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> tuple8DataStream = Two_CGroup(today_WO_BG, ycsb_lb_modelSplitStream, windowSizeMillis);
        //即将维保的
        DataStreamSource<Tuple2<Integer, Integer>> today_DP_JJWB = env.addSource(new DaPingSMZQ_WB()).setParallelism(1);
        //即将废弃的
        DataStreamSource<Tuple2<Integer, Integer>> today_DP_JJFQ = env.addSource(new DaPingSMZQ_FQ()).setParallelism(1);
        DataStream<Tuple3<Integer, Integer, Integer>> today_WB_FQ = First_CGroup(today_DP_JJWB, today_DP_JJFQ, windowSizeMillis);
        //(标志，维保，废弃，健康)
        DataStream<Tuple4<Integer, Integer, Integer, Integer>> tuple4DataStream = Three_CGroup(today_WB_FQ, zsStream, windowSizeMillis).map(new MyMapFunctionV4());
        //(标志，工单，变更，等级1，2，3，4，总和,维保，废弃，健康)
        DataStream<Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> tuple11DataStream = Four_CGroup(tuple8DataStream, tuple4DataStream, windowSizeMillis);
        tuple11DataStream.addSink(new MysqlSink_DP());
        tuple11DataStream.print("dapin:");
//        //30天告警分布
        DataStreamSource<Tuple3<String, String, Integer>> tuple3DataStreamSource = env.addSource(new DaPing_ZCGJFL_30()).setParallelism(1);
        tuple3DataStreamSource.addSink(new MysqlSink_DP_30D());
//        //资产大盘
        DataStreamSource<Tuple3<String, String, Integer>> tuple3DataStreamSource1 = env.addSource(new DaPingZCDP()).setParallelism(1);
        tuple3DataStreamSource1.addSink(new MysqlSink_DP_ZCDP());
    }

    private static DataStream<Tuple11<Integer,Integer, Integer,Integer,Integer,Integer, Integer,Integer,Integer,Integer, Integer>> Four_CGroup(
            //(标志，工单，变更，等级1，2，3，4，总和)
            DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> grades,
            //(标志，维保，废弃，健康)
            DataStream<Tuple4<Integer,Integer, Integer,Integer>> salaries,
            long windowSize) {
        DataStream<Tuple11<Integer,Integer, Integer,Integer,Integer,Integer, Integer,Integer,Integer,Integer, Integer>> apply = grades.coGroup(salaries)
                .where(new Four_Result_KeySelector_One())
                .equalTo(new Four_Result_KeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple4<Integer,Integer, Integer,Integer>, Tuple11<Integer,Integer, Integer,Integer,Integer,Integer, Integer,Integer,Integer,Integer, Integer>>() {
                    Tuple11<Integer,Integer, Integer,Integer,Integer,Integer, Integer,Integer,Integer,Integer, Integer> tuple11;

                    @Override
                    public void coGroup(Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> first, Iterable<Tuple4<Integer,Integer, Integer,Integer>> second, Collector<Tuple11<Integer,Integer, Integer,Integer,Integer,Integer, Integer,Integer,Integer,Integer, Integer>> collector) throws Exception {
                        tuple11 = new Tuple11<>();
                        for (Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> s : first) {
                            tuple11.f0=s.f0;
                            tuple11.f1 = s.f1;
                            tuple11.f2 = s.f2;
                            tuple11.f3 = s.f3;
                            tuple11.f4 = s.f4;
                            tuple11.f5 = s.f5;
                            tuple11.f6 = s.f6;
                            tuple11.f7 = s.f7;
                        }
                        for (Tuple4<Integer,Integer, Integer,Integer> s1 : second) {
                            tuple11.f8= s1.f1;
                            tuple11.f9= s1.f2;
                            tuple11.f10= s1.f3;
                        }
                        if(tuple11.f0==null){
                            tuple11.f0=0;
                        } if(tuple11.f1==null){
                            tuple11.f1=0;
                        }
                        if(tuple11.f2==null){
                            tuple11.f2=0;
                        }
                        if(tuple11.f3==null){
                            tuple11.f3=0;
                        }
                        if(tuple11.f4==null){
                            tuple11.f4=0;
                        } if(tuple11.f5==null){
                            tuple11.f5=0;
                        }
                        if(tuple11.f6==null){
                            tuple11.f6=0;
                        }
                        if(tuple11.f7==null){
                            tuple11.f7=0;
                        } if(tuple11.f8==null){
                            tuple11.f8=0;
                        }
                        if(tuple11.f9==null){
                            tuple11.f9=0;
                        }
                        if(tuple11.f10==null){
                            tuple11.f10=0;
                        }
                        collector.collect(tuple11);
                    }
                });
        return apply;
    }
    private static class Four_Result_KeySelector_One implements KeySelector<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Integer> {
        @Override
        public Integer getKey(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value) {
            return value.f0;
        }
    }

    private static class Four_Result_KeySelector implements KeySelector<Tuple4<Integer,Integer, Integer,Integer>, Integer> {
        @Override
        public Integer getKey(Tuple4<Integer,Integer, Integer,Integer> value) {
            return value.f0;
        }
    }


    @Slf4j
    static class MyMapFunctionV4 implements MapFunction<Tuple4<Integer, Integer,Integer, Integer>, Tuple4<Integer, Integer, Integer,Integer>> {
        @Override
        public Tuple4<Integer, Integer, Integer,Integer> map(Tuple4<Integer, Integer,Integer, Integer> sourceEvent) {
            int wb = sourceEvent.f1;
            int fq =  sourceEvent.f2;
            int ZN = sourceEvent.f3-wb-fq ;
            return Tuple4.of(sourceEvent.f0, wb, fq, ZN);
        }
    }
    private static DataStream<Tuple4<Integer,Integer, Integer,Integer>> Three_CGroup(
            DataStream<Tuple3<Integer,Integer, Integer>> grades,
            DataStream<Tuple2<Integer,Integer>> salaries,
            long windowSize) {
        DataStream<Tuple4<Integer,Integer, Integer,Integer>> apply = grades.coGroup(salaries)
                .where(new Two_Result_KeySelector_One())
                .equalTo(new First_Result_KeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<Tuple3<Integer,Integer, Integer>, Tuple2<Integer,Integer>, Tuple4<Integer,Integer, Integer,Integer>>() {
                    Tuple4<Integer,Integer, Integer,Integer> tuple4;

                    @Override
                    public void coGroup(Iterable<Tuple3<Integer,Integer, Integer>> first, Iterable<Tuple2<Integer,Integer>> second, Collector<Tuple4<Integer,Integer, Integer,Integer>> collector) throws Exception {
                        tuple4 = new Tuple4<>();
                        for (Tuple3<Integer,Integer,Integer> s : first) {
                            tuple4.f0=s.f0;
                            tuple4.f1 = s.f1;
                            tuple4.f2 = s.f2;
                        }
                        for (Tuple2<Integer,Integer> s1 : second) {
                            tuple4.f3= s1.f1;
                        }
                        if(tuple4.f0==null){
                            tuple4.f0=0;
                        }
                        if(tuple4.f1==null){
                            tuple4.f1=0;
                        }
                        if(tuple4.f2==null){
                            tuple4.f2=0;
                        }
                        if(tuple4.f3==null){
                            tuple4.f3=0;
                        }
                        collector.collect(tuple4);
                    }
                });
        return apply;
    }

    private static DataStream<Tuple8<Integer,Integer, Integer,Integer,Integer, Integer,Integer,Integer>> Two_CGroup(
            DataStream<Tuple3<Integer,Integer, Integer>> grades,
            DataStream<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> salaries,
            long windowSize) {
        DataStream<Tuple8<Integer,Integer, Integer,Integer,Integer, Integer,Integer,Integer>> apply = grades.coGroup(salaries)
                .where(new Two_Result_KeySelector_One())
                .equalTo(new Two_Result_KeySelector_two_1())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<Tuple3<Integer,Integer, Integer>, Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>, Tuple8<Integer,Integer, Integer,Integer,Integer, Integer,Integer,Integer>>() {
                    Tuple8<Integer,Integer, Integer,Integer,Integer, Integer,Integer,Integer> tuple8;

                    @Override
                    public void coGroup(Iterable<Tuple3<Integer,Integer, Integer>> first, Iterable<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> second, Collector<Tuple8<Integer,Integer, Integer,Integer,Integer, Integer,Integer,Integer>> collector) throws Exception {
                        tuple8 = new Tuple8<>();
                        for (Tuple3<Integer,Integer,Integer> s : first) {
                            tuple8.f0=s.f0;
                            tuple8.f1 = s.f1;
                            tuple8.f2 = s.f2;
                        }
                        for (Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> s1 : second) {
                            tuple8.f3= s1.f0;
                            tuple8.f4= s1.f1;
                            tuple8.f5= s1.f2;
                            tuple8.f6= s1.f3;
                            tuple8.f7= s1.f4;
                        }
                        if(tuple8.f0==null){
                            tuple8.f0=0;
                        }
                        if(tuple8.f1==null){
                            tuple8.f1=0;
                        }
                        if(tuple8.f2==null){
                            tuple8.f2=0;
                        }
                        if(tuple8.f3==null){
                            tuple8.f3=0;
                        }
                        if(tuple8.f4==null){
                            tuple8.f4=0;
                        }if(tuple8.f5==null){
                            tuple8.f5=0;
                        }if(tuple8.f6==null){
                            tuple8.f6=0;
                        }if(tuple8.f7==null) {
                            tuple8.f7 = 0;
                        }
                        collector.collect(tuple8);
                    }
                });
        return apply;
    }
    private static class Two_Result_KeySelector_One implements KeySelector<Tuple3<Integer,Integer, Integer>, Integer> {
        @Override
        public Integer getKey(Tuple3<Integer,Integer, Integer> value) {
            return value.f0;
        }
    }

    private static class Two_Result_KeySelector_two_1 implements KeySelector<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>, Integer> {
        @Override
        public Integer getKey(Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> value) {
            return value.f5;
        }
    }


    private static DataStream<Tuple3<Integer,Integer, Integer>> First_CGroup(
            DataStream<Tuple2<Integer, Integer>> grades,
            DataStream<Tuple2<Integer, Integer>> salaries,
            long windowSize) {
        DataStream<Tuple3<Integer,Integer, Integer>> apply = grades.coGroup(salaries)
                .where(new First_Result_KeySelector())
                .equalTo(new First_Result_KeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple3<Integer,Integer, Integer>>() {
                    Tuple3<Integer,Integer, Integer> tuple3;

                    @Override
                    public void coGroup(Iterable<Tuple2<Integer, Integer>> first, Iterable<Tuple2<Integer, Integer>> second, Collector<Tuple3<Integer,Integer, Integer>> collector) throws Exception {
                        tuple3 = new Tuple3<>();
                        for (Tuple2<Integer, Integer> s : first) {
                            tuple3.f0=s.f0;
                            tuple3.f1 = s.f1;
                        }
                        for (Tuple2<Integer, Integer> s1 : second) {
                            tuple3.f2= s1.f1;
                        }
                        if(tuple3.f0==null){
                            tuple3.f0=1;
                        }
                        if(tuple3.f1==null){
                            tuple3.f1=0;
                        }
                        if (tuple3.f2==null){
                            tuple3.f2=0;
                        }
                        collector.collect(tuple3);
                    }
                });
        return apply;
    }
    private static class First_Result_KeySelector implements KeySelector<Tuple2<Integer, Integer>, Integer> {
        @Override
        public Integer getKey(Tuple2<Integer, Integer> value) {
            return value.f0;
        }
    }


    private static DataStream<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> getYcsb_lb_modelSplitStream(DataStreamSource<Tuple2<String, Integer>> sum, long windowSize) {
        SplitStream<Tuple2<String, Integer>> split = sum.split((OutputSelector<Tuple2<String, Integer>>) event -> {
            List<String> output = new ArrayList<>();
            String type = event.f0.trim();
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
        DataStream<Tuple2<Integer, Integer>> select_1 = split.select("level_1").map(new MapFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return Tuple2.of(stringIntegerTuple2.f1,1);
            }
        });
        DataStream<Tuple2<Integer, Integer>> select_2 = split.select("level_2").map(new MapFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return Tuple2.of(stringIntegerTuple2.f1,1);
            }
        });
        DataStream<Tuple2<Integer, Integer>> select_3 = split.select("level_3").map(new MapFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return Tuple2.of(stringIntegerTuple2.f1,1);
            }
        });
        DataStream<Tuple2<Integer, Integer>> select_4 = split.select("level_4").map(new MapFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return Tuple2.of(stringIntegerTuple2.f1,1);
            }
        });
        DataStream<Tuple3<Integer,Integer,Integer>> ycsb_lb_result_modelDataStream = YCLB_Result_CGroup(select_1, select_2, windowSize);
        DataStream<Tuple3<Integer,Integer,Integer>> ycsb_lb_result_modelDataStream1 = YCLB_Result_CGroup(select_3, select_4, windowSize);
        DataStream<Tuple5<Integer,Integer,Integer,Integer,Integer>> modelSecondDataStream = YCLB_Finally_CGroup(ycsb_lb_result_modelDataStream, ycsb_lb_result_modelDataStream1, windowSize);
        SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> map = modelSecondDataStream.map(new MyMapFunctionV3());
        return map;
    }
    private static DataStream<Tuple3<Integer,Integer,Integer>> YCLB_Result_CGroup(
            DataStream<Tuple2<Integer, Integer>> grades,
            DataStream<Tuple2<Integer, Integer>> salaries,
            long windowSize) {
        DataStream<Tuple3<Integer,Integer,Integer>> apply = grades.coGroup(salaries)
                .where(new YCFB_Result_KeySelectorOne())
                .equalTo(new YCFB_Result_KeySelectorOne())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple3<Integer,Integer,Integer>>() {
                   Tuple3<Integer,Integer,Integer> tuple3=null;

                    @Override
                    public void coGroup(Iterable<Tuple2<Integer, Integer>> first, Iterable<Tuple2<Integer, Integer>> second, Collector<Tuple3<Integer,Integer,Integer>> collector) throws Exception {
                        tuple3 = new Tuple3<>();
                        for (Tuple2<Integer, Integer> s : first) {
                            tuple3.f0=s.f0;
                        }
                        for (Tuple2<Integer, Integer> s1 : second) {
                           tuple3.f1=s1.f0;
                           tuple3.f2=1;
                        }
                        if(tuple3.f0==null){
                            tuple3.f0=0;
                        }
                        if (tuple3.f1==null){
                            tuple3.f1=0;
                        }
                        if (tuple3.f2==null){
                            tuple3.f2=1;
                        }
                        collector.collect(tuple3);
                    }
                });
        return apply;
    }

    private static DataStream<Tuple5<Integer,Integer,Integer,Integer,Integer>> YCLB_Finally_CGroup(
            DataStream<Tuple3<Integer,Integer,Integer>> grades,
            DataStream<Tuple3<Integer,Integer,Integer>> salaries,
            long windowSize) {
        DataStream<Tuple5<Integer,Integer,Integer,Integer,Integer>> apply = grades.coGroup(salaries)
                .where(new YCFB_Finall_KeySelector())
                .equalTo(new YCFB_Finall_KeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<Tuple3<Integer,Integer,Integer>, Tuple3<Integer,Integer,Integer>, Tuple5<Integer,Integer,Integer,Integer,Integer>>() {
                   Tuple5<Integer,Integer,Integer,Integer,Integer> tuple5=null;

                    @Override
                    public void coGroup(Iterable<Tuple3<Integer,Integer,Integer>> first, Iterable<Tuple3<Integer,Integer,Integer>> second, Collector<Tuple5<Integer,Integer,Integer,Integer,Integer>> collector) throws Exception {
                        tuple5 = new Tuple5<>();
                        for (Tuple3<Integer,Integer,Integer> s : first) {
                           tuple5.f0=s.f0;
                           tuple5.f1=s.f1;
                        }
                        for (Tuple3<Integer,Integer,Integer> s : second) {
                           tuple5.f2 = s.f0;
                           tuple5.f3 = s.f1;
                           tuple5.f4 = 1;
                        }
                        if(tuple5.f0==null){
                            tuple5.f0=0;
                        }
                        if(tuple5.f1==null){
                            tuple5.f1=0;
                        }
                        if(tuple5.f2==null){
                            tuple5.f2=0;
                        }
                        if(tuple5.f3==null){
                            tuple5.f3=0;
                        }
                        collector.collect(tuple5);
                    }
                });
        return apply;
    }
    private static class YCFB_Result_KeySelector implements KeySelector<Tuple2<String, Integer>, Integer> {
        @Override
        public Integer getKey(Tuple2<String, Integer> value) {
            return Integer.parseInt(value.f0.trim());
        }
    }

    private static class YCFB_Result_KeySelectorOne implements KeySelector<Tuple2<Integer, Integer>, Integer> {
        @Override
        public Integer getKey(Tuple2<Integer, Integer> value) {
            return value.f1;
        }
    }

    private static class YCFB_Finall_KeySelector implements KeySelector<Tuple3<Integer,Integer,Integer>, Integer> {
        @Override
        public Integer getKey(Tuple3<Integer,Integer,Integer> value) {
            return value.f2;
        }
    }

    private static class YCFB_Finall_KeySelectorOne implements KeySelector<ModelFirst, Integer> {
        @Override
        public Integer getKey(ModelFirst value) {
            return Integer.parseInt(value.getLevel_1().trim());
        }
    }
    @Slf4j
    static class MyMapFunctionV3 implements MapFunction<Tuple5<Integer,Integer,Integer,Integer,Integer>, Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> {
        @Override
        public Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> map(Tuple5<Integer,Integer,Integer,Integer,Integer> sourceEvent) {
            Integer level_one = sourceEvent.f0;
            Integer level_two = sourceEvent.f1;
            Integer level_three = sourceEvent.f2;
            Integer level_four = sourceEvent.f3;
            Integer ZN = level_one + level_two + level_three + level_four;
            return Tuple6.of(level_one, level_two, level_three, level_four, ZN,1);
        }
    }

}
