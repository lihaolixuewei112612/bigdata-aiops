//package com.dtc.java.analytic.es6;
//
//
//import com.dtc.java.analytic.common.model.MetricEvent;
//import com.dtc.java.analytic.common.utils.ExecutionEnvUtil;
//import com.dtc.java.analytic.common.utils.KafkaConfigUtil;
//import com.dtc.java.analytic.es6.utils.ESSinkUtil;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
//import org.apache.http.HttpHost;
//import org.elasticsearch.client.Requests;
//import org.elasticsearch.common.xcontent.XContentType;
//
//import java.util.List;
//
//import static com.dtc.java.analytic.common.constant.PropertiesConstants.*;
//
//
///**
// * Created on 2019-12-20
// *
// * @author :hao.li
// */
//@Slf4j
//public class Sink2ES6Main {
//    public static void main(String[] args) throws Exception {
//        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
//        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
//        DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);
//
//        List<HttpHost> esAddresses = ESSinkUtil.getEsAddresses(parameterTool.get(ELASTICSEARCH_HOSTS));
//        int bulkSize = parameterTool.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
//        int sinkPåarallelism = parameterTool.getInt(STREAM_SINK_PARALLELISM, 5);
//
//        log.info("-----esAddresses = {}, parameterTool = {}, ", esAddresses, parameterTool);
//
//        ESSinkUtil.addSink(esAddresses, bulkSize, sinkParallelism, data,
//                (MetricEvent metric, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
//                    requestIndexer.add(Requests.indexRequest()
//                            .index(ZHISHENG + "_" + metric.getName())
//                            .type(ZHISHENG)
//                            .source(GsonUtil.toJSONBytes(metric), XContentType.JSON));
//                },
//                parameterTool);
//        env.execute("flink learning connectors es6");
//    }
//}
