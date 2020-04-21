package com.dtc.java.SC.JGPF;

import com.dtc.java.SC.JKZL.model.YCSB_LB_Model;
import com.dtc.java.SC.JKZL.model.YCSB_LB_RESULT_Finally_Model;
import com.dtc.java.SC.JKZL.model.YCSB_LB_RESULT_Model;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
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
 * Created on : 2020-03-27
 * @Description : 机房区域机柜界面异常设备列表
 */


public class test {

    public static void main(String[] args) throws Exception {
        final long windowSize = 1000L;
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        Map<String, String> stringStringMap = parameterTool.toMap();
        Properties properties = new Properties();
        for (String key : stringStringMap.keySet()) {
            if (key.startsWith("mysql")) {
                properties.setProperty(key, stringStringMap.get(key));
            }
        }

        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        //机房总设备数
        DataStreamSource<YCSB_LB_Model> tuple5DataStreamSource = env.addSource(new YCSB_LB()).setParallelism(1);//数据流定时从数据库中查出来数据
        SingleOutputStreamOperator<YCSB_LB_Model> sum = tuple5DataStreamSource.keyBy("room", "asset_id", "level_id").timeWindow(Time.milliseconds(windowSize)).sum("num");
        DataStream<YCSB_LB_RESULT_Finally_Model> ycsb_lb_modelSplitStream = getYcsb_lb_modelSplitStream(sum, windowSize);


        SingleOutputStreamOperator<YCSB_LB_Model> sum1 = tuple5DataStreamSource.keyBy("room", "partitions", "asset_id", "level_id").timeWindow(Time.milliseconds(windowSize)).sum("num");
        DataStream<YCSB_LB_RESULT_Finally_Model> ycsb_lb_modelSplitStream1 = getYcsb_lb_modelSplitStream(sum1, windowSize);
        SingleOutputStreamOperator<YCSB_LB_Model> sum2 = tuple5DataStreamSource.keyBy("room", "partitions", "box", "asset_id", "level_id").timeWindow(Time.milliseconds(windowSize)).sum("num");
        DataStream<YCSB_LB_RESULT_Finally_Model> ycsb_lb_modelSplitStream2 = getYcsb_lb_modelSplitStream(sum2, windowSize);
        env.execute("zhisheng broadcast demo");
    }

    private static DataStream<YCSB_LB_RESULT_Finally_Model> getYcsb_lb_modelSplitStream(SingleOutputStreamOperator<YCSB_LB_Model> sum,long windowSize) {
        SplitStream<YCSB_LB_Model> split = sum.split((OutputSelector<YCSB_LB_Model>) event -> {
            List<String> output = new ArrayList<>();
            String type = event.getLevel_id();
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
        DataStream<YCSB_LB_Model> select_1 = split.select("level_1");
        DataStream<YCSB_LB_Model> select_2 = split.select("level_2");
        DataStream<YCSB_LB_Model> select_3 = split.select("level_3");
        DataStream<YCSB_LB_Model> select_4 = split.select("level_4");
        DataStream<YCSB_LB_RESULT_Model> ycsb_lb_result_modelDataStream = YCLB_Result_CGroup(select_1, select_2, windowSize);
        DataStream<YCSB_LB_RESULT_Model> ycsb_lb_result_modelDataStream1 = YCLB_Result_CGroup(select_3, select_4, windowSize);
        DataStream<YCSB_LB_RESULT_Finally_Model> ycsb_lb_result_finally_modelDataStream = YCLB_Finally_CGroup(ycsb_lb_result_modelDataStream, ycsb_lb_result_modelDataStream1, windowSize);
        return ycsb_lb_result_finally_modelDataStream;
    }

    private static DataStream<YCSB_LB_RESULT_Model> YCLB_Result_CGroup(
            DataStream<YCSB_LB_Model> grades,
            DataStream<YCSB_LB_Model> salaries,
            long windowSize) {
        DataStream<YCSB_LB_RESULT_Model> apply = grades.coGroup(salaries)
                .where(new YCFB_Result_KeySelector())
                .equalTo(new YCFB_Result_KeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<YCSB_LB_Model, YCSB_LB_Model, YCSB_LB_RESULT_Model>() {
                    YCSB_LB_RESULT_Model ylrm = null;

                    @Override
                    public void coGroup(Iterable<YCSB_LB_Model> first, Iterable<YCSB_LB_Model> second, Collector<YCSB_LB_RESULT_Model> collector) throws Exception {
                        ylrm = new YCSB_LB_RESULT_Model();
                        for (YCSB_LB_Model s : first) {
                            String asset_id = s.getAsset_id();
                            ylrm.setAsset_id(asset_id);
                            ylrm.setName(s.getName());
                            ylrm.setIp(s.getIp());
                            ylrm.setRoom(s.getRoom());
                            ylrm.setPartitions(s.getPartitions());
                            ylrm.setBox(s.getBox());
                            ylrm.setLevel_1(s.getNum());
                        }
                        for (YCSB_LB_Model s1 : second) {
                            ylrm.setLevel_2(s1.getNum());
                        }
                        collector.collect(ylrm);
                    }
                });
        return apply;
    }
    private static DataStream<YCSB_LB_RESULT_Finally_Model> YCLB_Finally_CGroup(
            DataStream<YCSB_LB_RESULT_Model> grades,
            DataStream<YCSB_LB_RESULT_Model> salaries,
            long windowSize) {
        DataStream<YCSB_LB_RESULT_Finally_Model> apply = grades.coGroup(salaries)
                .where(new YCFB_Finall_KeySelector1())
                .equalTo(new YCFB_Finall_KeySelector1())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new CoGroupFunction<YCSB_LB_RESULT_Model, YCSB_LB_RESULT_Model, YCSB_LB_RESULT_Finally_Model>() {
                    YCSB_LB_RESULT_Finally_Model ylrm = null;

                    @Override
                    public void coGroup(Iterable<YCSB_LB_RESULT_Model> first, Iterable<YCSB_LB_RESULT_Model> second, Collector<YCSB_LB_RESULT_Finally_Model> collector) throws Exception {
                        ylrm = new YCSB_LB_RESULT_Finally_Model();
                        for (YCSB_LB_RESULT_Model s : first) {
                            String asset_id = s.getAsset_id();
                            ylrm.setAsset_id(asset_id);
                            ylrm.setName(s.getName());
                            ylrm.setIp(s.getIp());
                            ylrm.setRoom(s.getRoom());
                            ylrm.setPartitions(s.getPartitions());
                            ylrm.setBox(s.getBox());
                            ylrm.setLevel_1(s.getLevel_1());
                            ylrm.setLevel_2(s.getLevel_2());
                        }
                        for (YCSB_LB_RESULT_Model s : second) {
                            ylrm.setLevel_3(s.getLevel_1());
                            ylrm.setLevel_3(s.getLevel_2());
                        }
                        collector.collect(ylrm);
                    }
                });
        return apply;
    }


    private static class YCFB_Result_KeySelector implements KeySelector<YCSB_LB_Model, String> {
        @Override
        public String getKey(YCSB_LB_Model value) {
            return value.getRoom()+"_"+value.getPartitions()+"_"+value.getAsset_id();
        }
    }
    private static class YCFB_Finall_KeySelector1 implements KeySelector<YCSB_LB_RESULT_Model, String> {
        @Override
        public String getKey(YCSB_LB_RESULT_Model value) {
            return value.getRoom()+"_"+value.getPartitions()+"_"+value.getAsset_id();
        }
    }
}
