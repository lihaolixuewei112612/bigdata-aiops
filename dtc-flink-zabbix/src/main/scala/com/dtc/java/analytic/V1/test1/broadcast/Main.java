package com.dtc.java.analytic.V1.test1.broadcast;

import com.dtc.java.analytic.V1.common.schemas.MetricSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Properties;


/**
 * Created on 2019-12-30
 *
 * @author :hao.li
 */
@Slf4j
public class Main {

    final static MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
            "alarm_rules",
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO);

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        DataStreamSource<Map<String, String>> alarmDataStream = env.addSource(new GetAlarmNotifyData()).setParallelism(1);//数据流定时从数据库中查出来数据

        //test for get data from MySQL
//        alarmDataStream.print();

//        Properties props = buildKafkaProps(parameterTool);
//        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
//                "flink-test",
//                new SimpleStringSchema(),
//                props);
//        DataStreamSource<String> dataStreamSource = env.addSource(consumer);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.3.7.234:9092,10.3.7.233:9092,10.3.7.232:9092");// only required for Kafka 0.8properties.setProperty("zookeeper.connect", "localhost:2181");
//        properties.setProperty("group.id", "test");


        FlinkKafkaConsumer<MetricEvent> consumer = new FlinkKafkaConsumer<>(
                "flink-test",
                new MetricSchema(),
                properties);
        DataStreamSource<MetricEvent> dataStreamSource = env.addSource(consumer);
//        DataStreamSource<MetricEvent> metricEventDataStream = KafkaConfigUtil.buildSource(env);
//        dataStreamSource.map(e->e.getName()).print();
        SingleOutputStreamOperator<MetricEvent> alert = dataStreamSource.connect(alarmDataStream.broadcast(ALARM_RULES))
                .process(new BroadcastProcessFunction<MetricEvent, Map<String, String>, MetricEvent>() {
                    MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
                            "alarm_rules",
                            BasicTypeInfo.STRING_TYPE_INFO,
                            BasicTypeInfo.STRING_TYPE_INFO);
                    private MapStateDescriptor<String, String> alarmRulesMapStateDescriptor;

                    @Override
                    public void processElement(MetricEvent value, ReadOnlyContext ctx, Collector<MetricEvent> out) throws Exception {
                        log.info("5*************"+value.getName());

                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
                        Map<String, String> tags = value.getTags();
                        if (!tags.containsKey("desc")) {
                            log.info("6*******************");
                            return;
                        }
                        log.info("7*************************");
                        String targetId = broadcastState.get("desc");
                        log.info("000******************"+targetId);
                        if (targetId.equals(tags.get("desc"))) {
                            log.info("8*************************"+value.getTags().get("desc"));
                            value.getTags().put("desc", targetId); //将通知方式的 hook 放在 tag 里面，在下游要告警的时候通过该字段获取到对应的 hook 地址
                            out.collect(value);
                            log.info("9*************************"+value.getTags().get("desc"));
                        }else {
                            out.collect(value);
                        }
                    }

                    @Override
                    public void processBroadcastElement(Map<String, String> value, Context ctx, Collector<MetricEvent> out) throws Exception {
                        log.info("1*****************"+value.toString());
                        if (value != null) {
                            log.info("2*****************"+value.toString());

                            BroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
                            for (Map.Entry<String, String> entry : value.entrySet()) {
                                log.info("3*****************"+value.toString());
                                broadcastState.put(entry.getKey(), entry.getValue());
                                log.info("4*****************"+entry.getKey());
                                log.info("4.1*****************"+entry.getValue());

                            }
                        }
                    }
                });
        SingleOutputStreamOperator<MetricEvent> filter = alert.filter(e -> {
            String desc = e.getTags().get("desc");
            log.info("10**************"+desc);
            if (desc.equals("usb插入")) {
                return false;
            }else {
                return true;
            }
        });
        filter.map(e->e.getName()).print("finally");

        //其他的业务逻辑
        //alert.

        //然后在下游的算子中有使用到 alarmNotifyMap 中的配置信息


        env.execute("zhisheng broadcast demo");
    }
}
