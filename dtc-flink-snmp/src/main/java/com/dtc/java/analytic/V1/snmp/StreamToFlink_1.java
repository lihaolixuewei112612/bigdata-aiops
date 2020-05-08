package com.dtc.java.analytic.V1.snmp;

/**
 * @Author : lihao
 * Created on : 2020-05-08
 * @Description : TODO描述类作用
 */

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.dtc.java.analytic.V1.configuration.Configuration;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamToFlink_1 {
    public static final Logger logger = LoggerFactory.getLogger(StreamToFlink.class);

    public static Map totalMap = new HashMap<>();

    public static void main(String[] args) throws Exception {
        int windowSizeMillis = 4000;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(60000L);
        env.setParallelism(6);
        Properties prop = Configuration.getConf("kafka-flink.properties");
        String TOPIC = prop.get("topic").toString();
        FlinkKafkaConsumer myConsumer = new FlinkKafkaConsumer(TOPIC, (DeserializationSchema)new SimpleStringSchema(), prop);
        myConsumer.setStartFromLatest();
        myConsumer.assignTimestampsAndWatermarks(new DtcPeriodicAssigner());
        DataStreamSource<String> dataStreamSource = env.addSource((SourceFunction)myConsumer);
        WindowedStream<Tuple5<Tuple2<String, String>, String, String, String, String>, Tuple, TimeWindow> tuple5TupleTimeWindowWindowedStream = dataStreamSource.map(new MyMapFunction()).keyBy(new int[] { 0 }).timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS));
        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, String>> process = tuple5TupleTimeWindowWindowedStream.process(new myProcessWindowFunction());
        String str = "http://10.10.58.16:4399";
        process.addSink((SinkFunction)new SinkToOpentsdb(str));
        env.execute("PressureTest-start");
    }
}
