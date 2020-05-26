//package com.dtc.java.analytic.V2.source.mysql;
//
//import com.dtc.java.analytic.V2.common.model.DataStruct;
//import com.dtc.java.analytic.V2.common.utils.ExecutionEnvUtil;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.state.ListState;
//import org.apache.flink.api.common.state.ListStateDescriptor;
//import org.apache.flink.api.java.tuple.*;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.table.runtime.aggregate.TimeWindowPropertyCollector;
//import org.apache.flink.util.Collector;
//
//import java.sql.Timestamp;
//import java.util.*;
//
//
///**
// * Created on 2019-12-30
// *
// * @author :hao.li
// */
//public class test {
//
//    public static void main(String[] args) throws Exception {
//        int windowSizeMillis = 6000;
//
//        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
//        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
//
////        DataStreamSource<Map<String, String>> alarmDataStream = env.addSource(new ReadAlarmMessage()).setParallelism(1);//数据流定时从数据库中查出来数据
//        DataStreamSource<Tuple7<String, String, String, String, Double, String, String>> tuple7DataStreamSource = env.addSource(new GetAlarmNotify_Test()).setParallelism(1);//数据流定时从数据库中查出来数据
//        SingleOutputStreamOperator<Map<String, Tuple7<String, String, String, Double, Double, Double, Double>>> process = tuple7DataStreamSource.keyBy(0, 5).timeWindow(Time.milliseconds(windowSizeMillis)).process(new MySqlProcessMapFunction());
//        SingleOutputStreamOperator<Map<String, String>> map = process.map(new MySQLFunction());
//        map.map(new MapFunction<Map<String, String>, String>() {
//            @Override
//            public String map(Map<String, String> stringStringMap) throws Exception {
//                for(Map.Entry<String,String> str : stringStringMap.entrySet()){
//                    String key = str.getKey();
//                    String value = str.getValue();
//                }
//                return "abc";
//            }
//        });
////DPComplete for get data from MySQL
////        tuple7DataStreamSource.print();
//        env.execute("zhisheng broadcast demo");
//    }
//
//    static class MySQLFunction implements MapFunction<Map<String, Tuple7<String, String, String, Double, Double, Double, Double>>, Map<String, String>> {
//        //(445,10.3.1.6,101_101_106_103,50.0,null,null,null)
//
//        @Override
//        public  Map<String, String> map(Map<String, Tuple7<String, String, String, Double, Double, Double, Double>> event) throws Exception {
//            Map<String,String> map = new HashMap<>();
//            for(Map.Entry<String, Tuple7<String, String, String, Double, Double, Double, Double>> entries:event.entrySet()){
//                Tuple7<String, String, String, Double, Double, Double, Double> value = entries.getValue();
//                String key = entries.getKey();
//                String asset_id=value.f0;
//                String ip =value.f1;
//                String code =value.f2;
//                Double level_1=value.f3;
//                Double level_2=value.f4;
//                if(level_1 !=null){
//
//                }
//                Double level_3=value.f5;
//                Double level_4=value.f6;
//                String str = asset_id+":"+code+":"+level_1+"|"+level_2+"|"+level_3+"|"+level_4;
//              map.put(key,str);
//            }
//            return map;
//        }
//    }
//
//    @Slf4j
//    static class MySqlProcessMapFunction extends ProcessWindowFunction<Tuple7<String, String, String, String, Double, String, String>, Map<String, Tuple7<String, String, String, Double, Double, Double, Double>>, Tuple, TimeWindow> {
//        @Override
//        public void process(Tuple tuple, Context context, Iterable<Tuple7<String, String, String, String, Double, String, String>> iterable, Collector<Map<String, Tuple7<String, String, String, Double, Double, Double, Double>>> collector) throws Exception {
//            Tuple7<String, String, String, Double, Double, Double, Double> tuple7 = new Tuple7<>();
//            Map<String, Tuple7<String, String, String, Double, Double, Double, Double>> map = new HashMap<>();
//            for (Tuple7<String, String, String, String, Double, String, String> sourceEvent : iterable) {
//                String asset_id = sourceEvent.f0;
//                String ip = sourceEvent.f1;
//                Double num = sourceEvent.f4;
//                String code = sourceEvent.f5;
//                String level = sourceEvent.f6;
//                tuple7.f0 = asset_id;
//                tuple7.f1 = ip;
//                tuple7.f2 = code;
//                String key = ip + "." + code.replace("_",".");
//                if ("1".equals(level)) {
//                    tuple7.f3 = num;
//                } else if ("2".equals(level)) {
//                    tuple7.f4 = num;
//                } else if ("3".equals(level)) {
//                    tuple7.f5 = num;
//                } else if ("4".equals(level)) {
//                    tuple7.f6 = num;
//                }
//                map.put(key, tuple7);
//            }
//            collector.collect(map);
//        }
//
//    }
////    /** 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串 */
////    public static class TopNHotItems extends KeyedProcessFunction<Tuple, Tuple9<String, String, String,String, String, Double, Double, Double, Double>, String> {
////
////        private final int topSize;
////
////        public TopNHotItems(int topSize) {
////            this.topSize = topSize;
////        }
////
////        // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
////        private ListState<Tuple9<String, String, String,String, String, Double, Double, Double, Double>> itemState;
////
////        @Override
////        public void open(Configuration parameters) throws Exception {
////            super.open(parameters);
////            ListStateDescriptor<Tuple9<String, String, String,String, String, Double, Double, Double, Double>> itemsStateDesc = new ListStateDescriptor<>(
////                    "itemState-state",
////                    Tuple9.class);
////            itemState = getRuntimeContext().getListState(itemsStateDesc);
////        }
////
////        @Override
////        public void processElement(
////                Tuple9<String, String, String,String, String, Double, Double, Double, Double> input,
////                Context context,
////                Collector<String> collector) throws Exception {
////
////            // 每条数据都保存到状态中
////            itemState.add(input);
////            // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
////            context.timerService().registerEventTimeTimer(input.windowEnd + 1);
////        }
////
////        @Override
////        public void onTimer(
////                long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
////            // 获取收到的所有商品点击量
////            List<Tuple9<String, String, String,String, String, Double, Double, Double, Double>> allItems = new ArrayList<>();
////            for (Tuple9<String, String, String,String, String, Double, Double, Double, Double> item : itemState.get()) {
////                allItems.add(item);
////            }
////            // 提前清除状态中的数据，释放空间
////            itemState.clear();
////            // 按照点击量从大到小排序
////            allItems.sort(new Comparator<Tuple9<String, String, String,String, String, Double, Double, Double, Double>>() {
////                @Override
////                public int compare(Tuple9<String, String, String,String, String, Double, Double, Double, Double> o1, Tuple9<String, String, String,String, String, Double, Double, Double, Double> o2) {
////                    return (int) (o2.viewCount - o1.viewCount);
////                }
////            });
////            // 将排名信息格式化成 String, 便于打印
////            StringBuilder result = new StringBuilder();
////            result.append("====================================\n");
////            result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
////            for (int i=0; i<allItems.size() && i < topSize; i++) {
////                Tuple9<String, String, String,String, String, Double, Double, Double, Double> currentItem = allItems.get(i);
////                // No1:  商品ID=12224  浏览量=2413
////                result.append("No").append(i).append(":")
////                        .append("  商品ID=").append(currentItem.itemId)
////                        .append("  浏览量=").append(currentItem.viewCount)
////                        .append("\n");
////            }
////            result.append("====================================\n\n");
////
////            // 控制输出频率，模拟实时滚动结果
////            Thread.sleep(1000);
////
////            out.collect(result.toString());
////        }
////    }
//}
