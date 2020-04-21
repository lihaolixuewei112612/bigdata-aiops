package com.dtc.java.analytic.V1.sink.opentsdb;

import org.apache.flink.api.java.tuple.Tuple7;
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
public class PSinkToOpentsdb extends RichSinkFunction<Tuple7<String, String, String, String, String, Long, String>> {
    private static final Logger logger = LoggerFactory.getLogger(PSinkToOpentsdb.class);
    String properties;
    HttpClientImpl client;

    public PSinkToOpentsdb(String prop) {
        this.properties = prop;
    }

    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = new HttpClientImpl(properties);
    }

    public void close() {

    }

    public void invoke(Tuple7<String, String, String, String, String, Long, String> value) {
        String ip;
        String host;
        String oid;
        String name;
        Long time = 0L;
        double result = 0d;
        String TypeCS;
        String TypeSB;
        try {
            //写入OpenTSDB
            MetricBuilder builder = MetricBuilder.getInstance();
            ip = value.f0.trim();
            host = value.f1.trim();
            oid = value.f2.trim();
            if (oid.isEmpty()) {
                oid = "dtc-null";
            }
//            String[] s = value.f3.trim().split("_", 3);
//            TypeCS = s[0];
//            TypeSB = s[1];
//            name = s[2];
            name=value.f3.trim();
            time = value.f5;
            result = Double.parseDouble(value.f6);

            String metrics = ip + "_" + name;
//            String metrics = ip + "_" + host;
            builder.addMetric(metrics)
                    .setDataPoint(time, result)
                    .addTag("host", host)
                    .addTag("oid", oid)
                    .addTag("name",name);
//                    .addTag("TypeCS", TypeCS)
//                    .addTag("TypeSB", TypeSB);
            Response response = client.pushMetrics(builder, ExpectResponse.SUMMARY);
            if (!response.isSuccess()) {
                logger.error("Insert data to opentsdb is mistake,find the reason please!");
            }
        } catch (IOException e) {
            logger.error("insert data to opentsdb is mistake and the reason is {}.", e);
        }
    }
}
