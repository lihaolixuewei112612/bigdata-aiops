package com.dtc.java.analytic.V1.common.utils;


import com.dtc.java.analytic.V1.common.constant.PropertiesConstants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Created on 2019-10-17
 *
 * @author :hao.li
 */
public class KafkaConfigUtil {

    /**
     * 设置基础的 Kafka 配置
     *
     * @return
     */
    public static Properties buildKafkaProps() {
        return buildKafkaProps(ParameterTool.fromSystemProperties());
    }

    /**
     * 设置 kafka 配置
     *
     * @param parameterTool
     * @return
     */
    public static Properties buildKafkaProps(ParameterTool parameterTool) {
        Properties props = parameterTool.getProperties();
        props.put(PropertiesConstants.BOOTSTRAP_SERVERS, parameterTool.get(PropertiesConstants.KAFKA_BROKERS, PropertiesConstants.DEFAULT_KAFKA_BROKERS));
        props.put(PropertiesConstants.ZOOKEEPER_CONNECT, parameterTool.get(PropertiesConstants.KAFKA_ZOOKEEEPER_CONNECT, PropertiesConstants.DEFAULT_KAFKA_ZOOKEEPER_CONNECT));
        props.put(PropertiesConstants.GROUP_ID, parameterTool.get(PropertiesConstants.KAFKA_GROUP_ID, PropertiesConstants.DEFAULT_KAFKA_GROUP_ID));
        props.put(PropertiesConstants.TOPIC,parameterTool.get(PropertiesConstants.KAFKA_TOPIC,PropertiesConstants.DEFAULT_TOPIC));
        props.put(PropertiesConstants.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(PropertiesConstants.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(PropertiesConstants.AUTO_OFFSET_RESET, "latest");
        props.put(PropertiesConstants.DRIVER_NAME,parameterTool.get(PropertiesConstants.DTC_DRIVER_NAME,PropertiesConstants.DEFAULT_DRIVER_NAME));
        props.put(PropertiesConstants.JDBC_URL,parameterTool.get(PropertiesConstants.DTC_JDBC_URL,PropertiesConstants.DEFAULT_JDBC_URL));
        props.put(PropertiesConstants.USERNAME,parameterTool.get(PropertiesConstants.DTC_USERNAME,PropertiesConstants.DEFAULT_USERNAME));
        props.put(PropertiesConstants.PASSWORD,parameterTool.get(PropertiesConstants.DTC_PASSWORD,PropertiesConstants.DEFAULT_PASSWORD));
        props.put(PropertiesConstants.SQL,parameterTool.get(PropertiesConstants.DTC_SQL,PropertiesConstants.DEFAULT_DTC_SQL));
        return props;
    }


    public static DataStreamSource<String> buildSource(StreamExecutionEnvironment env) throws IllegalAccessException {
        ParameterTool parameter = (ParameterTool) env.getConfig().getGlobalJobParameters();
        String topic = parameter.get(PropertiesConstants.KAFKA_TOPIC);
        Long time = parameter.getLong(PropertiesConstants.CONSUMER_FROM_TIME, 0L);
        return buildSource(env, topic, time);
    }

    /**
     * @param env
     * @param topic
     * @param time  订阅的时间
     * @return
     * @throws IllegalAccessException
     */
    public static DataStreamSource<String> buildSource(StreamExecutionEnvironment env, String topic, Long time){
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
        Properties props = buildKafkaProps(parameterTool);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        consumer.setStartFromLatest();
        //重置offset到time时刻
        if (time != 0L) {
            Map<KafkaTopicPartition, Long> partitionOffset = buildOffsetByTime(props, parameterTool, time);
            consumer.setStartFromSpecificOffsets(partitionOffset);
        }
        return env.addSource(consumer);
    }

    private static Map<KafkaTopicPartition, Long> buildOffsetByTime(Properties props, ParameterTool parameterTool, Long time) {
        props.setProperty("group.id", "query_time_" + time);
        KafkaConsumer consumer = new KafkaConsumer(props);
        List<PartitionInfo> partitionsFor = consumer.partitionsFor(parameterTool.getRequired(PropertiesConstants.METRICS_TOPIC));
        Map<TopicPartition, Long> partitionInfoLongMap = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionsFor) {
            partitionInfoLongMap.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), time);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsetResult = consumer.offsetsForTimes(partitionInfoLongMap);
        Map<KafkaTopicPartition, Long> partitionOffset = new HashMap<>();
        offsetResult.forEach((key, value) -> partitionOffset.put(new KafkaTopicPartition(key.topic(), key.partition()), value.offset()));

        consumer.close();
        return partitionOffset;
    }

//    public static SingleOutputStreamOperator<MetricEvent> parseSource(DataStreamSource<MetricEvent> dataStreamSource) {
//        return dataStreamSource.assignTimestampsAndWatermarks(new MetricWatermark());
//    }
}
