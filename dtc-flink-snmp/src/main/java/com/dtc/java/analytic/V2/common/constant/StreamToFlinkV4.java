package com.dtc.java.analytic.V2.common.constant;


import com.dtc.java.analytic.V2.common.model.DataStruct;
import com.dtc.java.analytic.V2.common.model.SourceEvent;
import com.dtc.java.analytic.V2.common.utils.ExecutionEnvUtil;
import com.dtc.java.analytic.V2.common.utils.KafkaConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created on 2019-08-12
 *
 * @author :ren
 */
@Slf4j
public class StreamToFlinkV3 {

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        int windowSizeMillis = parameterTool.getInt("dtc.windowSizeMillis", 2000);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.getConfig().setGlobalJobParameters(parameterTool);


//        DataStreamSource<SourceEvent> streamSource = env.addSource(new TestSourceEvent());
        DataStreamSource<SourceEvent> streamSource = KafkaConfigUtil.buildSource(env);

        /**
         * {"time":"1581691002687","code":"101_101_107_105_105","host":"10.3.7.234","nameCN":"磁盘剩余大小","value":"217802544","nameEN":"disk_free"}
         * */
//        DataStreamSource<String> dataStreamSource = env.socketTextStream("172.20.10.2", 8080, '\n');

        DataStream<DataStruct> mapStream = streamSource.map(new MyMapFunctionV3());
        DataStream<DataStruct> timeSingleOutputStream
                = mapStream.assignTimestampsAndWatermarks(new DtcPeriodicAssigner());
        timeSingleOutputStream.map(new WinMapFunction()).filter(new FilterFunction<DataStruct>(){

            @Override
            public boolean filter(DataStruct dataStruct) throws Exception {
                boolean strResult = dataStruct.getValue().matches("-?[0-9]+.*[0-9]*");
//                return strResult;
                String str ="101_101_101_106_106";
                boolean equals=false;
                if(dataStruct.getZbFourName().equals(str)){
                     equals = true;
                }
                return !equals;
            }
        }).keyBy(new KeySelector<DataStruct, String>() {
            @Override
            public String getKey(DataStruct value) throws Exception {
                return value.getHost();
            }
            //时间窗口 6秒  滑动间隔3秒
        })
                .timeWindow(Time.seconds(6))
                .aggregate(new CountAggregate(),new CountWindowFunction()).print("test:");
        env.execute("Dtc-Alarm-Flink-Process");
    }
}

class CountWindowFunction implements WindowFunction<Double, String, String, TimeWindow> {
    @Override
    public void apply(String productId, TimeWindow window, Iterable<Double> input, Collector<String> out) throws Exception {
        /*商品访问统计输出*/
        /*out.collect("productId"productId,window.getEnd(),input.iterator().next()));*/
        out.collect("----------------窗口时间：" + window.getEnd());
        out.collect("商品ID: " + productId + "  浏览量: " + input.iterator().next());
    }
}

class CountAggregate implements AggregateFunction<DataStruct, Tuple2<String, Double>, Double> {
    @Override
    public Tuple2 createAccumulator() {
        /*访问量初始化为0*/
        return Tuple2.of("", 0D);
    }

    @Override
    public Tuple2<String, Double> add(DataStruct value, Tuple2<String, Double> acc) {
        /*访问量直接+1 即可*/
        return new Tuple2<>(value.getHost() + ":" + value.getZbFourName(), acc.f1 + Double.parseDouble(value.getValue()));
    }

    @Override
    public Double getResult(Tuple2<String, Double> acc) {
        return acc.f1;
    }

    @Override
    public Tuple2<String, Double> merge(Tuple2<String, Double> longLongTuple2, Tuple2<String, Double> acc1) {
        return new Tuple2<>(longLongTuple2.f0, longLongTuple2.f1 + acc1.f1);
    }
}


@Slf4j
class MyMapFunctionV3 implements MapFunction<SourceEvent, DataStruct> {
    @Override
    public DataStruct map(SourceEvent sourceEvent) {
        String[] codes = sourceEvent.getCode().split("_");
        String systemName = codes[0].trim() + "_" + codes[1].trim();
        String zbFourCode = systemName + "_" + codes[2].trim() + "_" + codes[3].trim();
        String zbLastCode = codes[4].trim();
        String nameCN = sourceEvent.getNameCN();
        String nameEN = sourceEvent.getNameEN();
        String time = sourceEvent.getTime();
        String value = sourceEvent.getValue();
        String host = sourceEvent.getHost();
        return new DataStruct(systemName, host, zbFourCode, zbLastCode, nameCN, nameEN, time, value);
    }
}

class WinMapFunction implements MapFunction<DataStruct, DataStruct> {

    @Override
    public DataStruct map(DataStruct event) throws Exception {
        String zbLastCode = event.getZbLastCode();

        if (zbLastCode.contains(".")) {
            String lastCode = zbLastCode.split("\\.", 2)[0];
            String nameCode = zbLastCode.split("\\.", 2)[1];
            String result = event.getZbFourName() + "_" + lastCode;
            return new DataStruct(event.getSystem_name() + "|win_0",event.getHost(),result,nameCode,event.getNameCN(),event.getNameEN(),event.getTime(),event.getValue());

        } else {
            String result = event.getZbFourName() + "_" + zbLastCode;
            return new DataStruct(event.getSystem_name() + "|win_1",event.getHost(),result,"",event.getNameCN(),event.getNameEN(),event.getTime(),event.getValue());

        }
    }
}

