package com.dtc.java.SC.JSC;//package com.dtc.java.shucang.JaShiCang;
//
//import com.dtc.java.shucang.JFSBWGBGJ.ExecutionEnvUtil;
//import com.dtc.java.shucang.daping.source.*;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.Map;
//import java.util.Properties;
//
//
///**
// * @Author : lihao
// * Created on : 2020-03-24
// * @Description : 数仓监控大盘指标总类
// */
//public class DPComplete {
//
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
//        DataStreamSource<Integer> zsStream = env.addSource(new DaPingAllNum()).setParallelism(1);
//        DataStreamSource<Integer> zsStreamAlarm = env.addSource(new DaPingAlarm()).setParallelism(1);
//        DataStreamSource<Integer> zsStreamOrder = env.addSource(new DaPingOrder()).setParallelism(1);
//        DataStreamSource<Integer> zsStreamBGOrder = env.addSource(new DaPingBianGengOrder()).setParallelism(1);
//        DataStreamSource<Integer> zsStreamZCNum = env.addSource(new DaPingZCAllNum()).setParallelism(1);
//        DataStreamSource<Tuple2<String,Integer>> DaPingWCLAlarm = env.addSource(new DaPingWCLAlarm()).setParallelism(1);
//        DataStreamSource<Tuple2<String,Integer>> DaPingGJFB = env.addSource(new DaPingGJFB()).setParallelism(1);
//        DaPingGJFB.print();
//        env.execute("com.dtc.java.SC sart");
//    }
//}
