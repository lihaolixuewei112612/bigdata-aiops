package com.dtc.java.analytic.V2.common.constant;

/**
 * @author :hao.li
 */
public class PropertiesConstants {

    /**
     * key
     */
    public static final String PROPERTIES_FILE_NAME = "/kafka-flink_test.properties";

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";

    public static final String TOPIC = "topic";

    public static final String GROUP_ID = "group.id";

    public static final String KEY_DESERIALIZER = "key.deserializer";

    public static final String VALUE_DESERIALIZER = "value.deserializer";

    public static final String AUTO_OFFSET_RESET = "auto.offset.reset";

    public static final String KAFKA_BROKERS = "dtc.bootstrap.servers";
    public static final String KAFKA_ZOOKEEEPER_CONNECT="dtc.zookeeper.connect";
    public static final String KAFKA_GROUP_ID="dtc.group.id";
    public static final String KAFKA_TOPIC="dtc.topic";

    public static final String OPENTSDB_URL="dtc.opentsdb.url";

    public static final String METRICS_TOPIC = "metrics.topic";

    public static final String CONSUMER_FROM_TIME = "consumer.from.time";

    public static final String STREAM_PARALLELISM = "stream.parallelism";

    public static final String STREAM_SINK_PARALLELISM = "stream.sink.parallelism";

    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";

    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";

    public static final String STREAM_DEFAULT_PARALLELISM = "stream.default.parallelism";

    public static final String USERNAME = "mysql.user";

    public static final String PASSWORD = "mysql.password";

    public static final String DRIVER_NAME = "mysql.driver";

    public static final String JDBC_URL = "mysql.url";

    public static final String SQL = "mysql.sql";

    public static final String DTC_USERNAME = "dtc.mysql.user";

    public static final String DTC_PASSWORD = "dtc.mysql.password";

    public static final String DTC_DRIVER_NAME = "dtc.mysql.driver";

    public static final String DTC_JDBC_URL = "dtc.mysql.url";

    public static final String DTC_SQL = "dtc.mysql.sql";

    public static final String INITIA_SIZE = "mysql.initialSize";

    public static final String MAX_TOTAL = "mysql.maxTotal";

    public static final String MIN_IDLE = "mysql.minIdle";

    public static final String CHAR_UTF8 = "UTF-8";

    public static final String ALARM_CONVERGENCE="dtc.alarm.convergence.times";


    /**
     * default value
     */
    public static final String DEFAULT_KAFKA_BROKERS = "localhost:9092";

    public static final String KAFKA_ZOOKEEPER_CONNECT = "kafka.zookeeper.connect";

    public static final String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "localhost:2181";

    public static final String DEFAULT_KAFKA_GROUP_ID = "localgroup";

    public static final String DEFAULT_TOPIC="dtc";

    public static final String DEFAULT_USERNAME = "root";

    public static final String DEFAULT_PASSWORD = "123456";

    public static final String DEFAULT_DRIVER_NAME = "com.mysql.jdbc.Driver";

    public static final String DEFAULT_JDBC_URL = "jdbc:mysql://localhost:3306/click_traffic?useUnicode=true&characterEncoding=UTF-8";

    public static final String DEFAULT_DTC_SQL ="select * from DPComplete";

    public static final int DEFAULT_INITIA_SIZE = 10;

    public static final int DEFAULT_MAX_TOTAL = 50;

    public static final int DEFAULT_MIN_IDLE = 5;

    public static final String DEFAULT_CHAR_UTF8 = "UTF-8";

    public static final String CEP = "CEP";

    public static final String TU_BING = "Tubing";

    public static final String CASSANDRA = "Cassandra";

    public static final String TU_BING_SINK = "TubingSink";

    public static final String CASSANDRA_SINK = "CassandraSink";

    public static final String CASSANDRA_HOST_1 = "cassandra.host.1";

    public static final String CASSANDRA_HOST_2 = "cassandra.host.2";

    //es config
    public static final String ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS = "elasticsearch.bulk.flush.max.actions";
    public static final String ELASTICSEARCH_HOSTS = "elasticsearch.hosts";

    //mysql
    public static final String MYSQL_DATABASE = "mysql.database";
    public static final String MYSQL_HOST = "mysql.host";
    public static final String MYSQL_PASSWORD = "mysql.password";
    public static final String MYSQL_PORT = "mysql.port";
    public static final String MYSQL_USERNAME = "mysql.username";
    public static final String MYSQL_ALAEM_TABLE = "mysql.alarm_rule_table";
    public static final String MYSQL_WINDOWS_TABLE = "mysql.windows_disk_sql";

    //redis
    public static final String REDIS_IP="dtc.redis.ip";
    public static final String REDIS_PORT="dtc.redis.port";
    public static final String REDIS_PW="dtc.redis.pw";


}
