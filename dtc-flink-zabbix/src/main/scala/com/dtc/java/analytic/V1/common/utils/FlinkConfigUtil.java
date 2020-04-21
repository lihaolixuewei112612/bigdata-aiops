package com.dtc.java.analytic.V1.common.utils;

import com.dtc.java.analytic.V1.common.constant.PropertiesConstants;
import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

/**
 * Created on 2019-08-16
 *
 * @author :hao.li
 */
public class FlinkConfigUtil {
    public static Properties buildKafkaProps(Configuration conf) {
        Properties props = new Properties();
        props.put(PropertiesConstants.BOOTSTRAP_SERVERS, conf.get(PropertiesConstants.KAFKA_BROKERS, PropertiesConstants.DEFAULT_KAFKA_BROKERS));
        props.put(PropertiesConstants.ZOOKEEPER_CONNECT, conf.get(PropertiesConstants.KAFKA_ZOOKEEEPER_CONNECT, PropertiesConstants.DEFAULT_KAFKA_ZOOKEEPER_CONNECT));
        props.put(PropertiesConstants.GROUP_ID, conf.get(PropertiesConstants.KAFKA_GROUP_ID, PropertiesConstants.DEFAULT_KAFKA_GROUP_ID));
        props.put(PropertiesConstants.TOPIC,conf.get(PropertiesConstants.KAFKA_TOPIC,PropertiesConstants.DEFAULT_TOPIC));
        props.put(PropertiesConstants.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(PropertiesConstants.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(PropertiesConstants.AUTO_OFFSET_RESET, "latest");
        props.put(PropertiesConstants.DRIVER_NAME,conf.get(PropertiesConstants.DTC_DRIVER_NAME,PropertiesConstants.DEFAULT_DRIVER_NAME));
        props.put(PropertiesConstants.JDBC_URL,conf.get(PropertiesConstants.DTC_JDBC_URL,PropertiesConstants.DEFAULT_JDBC_URL));
        props.put(PropertiesConstants.USERNAME,conf.get(PropertiesConstants.DTC_USERNAME,PropertiesConstants.DEFAULT_USERNAME));
        props.put(PropertiesConstants.PASSWORD,conf.get(PropertiesConstants.DTC_PASSWORD,PropertiesConstants.DEFAULT_PASSWORD));
        props.put(PropertiesConstants.SQL,conf.get(PropertiesConstants.DTC_SQL,PropertiesConstants.DEFAULT_DTC_SQL));
        return props;
    }
}
