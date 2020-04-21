package com.dtc.java.analytic.V1.snmp;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.opentsdb.client.ExpectResponse;
import org.opentsdb.client.HttpClientImpl;
import org.opentsdb.client.builder.MetricBuilder;
import org.opentsdb.client.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created on 2019-08-14
 *
 * @author :hao.li
 */
public class SinkToOpentsdb extends RichSinkFunction<Tuple6<String, String, String, String, String, Double>> {
    private static final Logger logger = LoggerFactory.getLogger(SinkToOpentsdb.class);
    String properties;
    HttpClientImpl httpClient;

    public SinkToOpentsdb(String prop) {
        this.properties = prop;
    }

    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        httpClient = new HttpClientImpl(properties);
    }

    public void invoke(Tuple6<String, String, String, String, String, Double> value) {
        try {
            //写入OpenTSDB
            MetricBuilder builder = MetricBuilder.getInstance();
            builder.addMetric("wangxu")
                    .setDataPoint(value.f5)
                    .addTag("host_type", "test");
            Response response1 = httpClient.pushMetrics(builder, ExpectResponse.SUMMARY);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
