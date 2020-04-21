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

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created on 2019-08-14
 *
 * @author :renzhicheng
 */
public class SinkToOpentsdb extends RichSinkFunction<Tuple6<String, String, String, String, String, String>> {
    private static final Logger logger = LoggerFactory.getLogger(SinkToOpentsdb.class);

    //锐捷指标库
    public static final ArrayList<String> RG_AS224GT = new ArrayList<String>(Arrays.asList("102_107_101_101", "102_107_102_102"));

    String properties;
    HttpClientImpl httpClient;

    public SinkToOpentsdb(String prop) {
        this.properties = prop;
    }

    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        httpClient = new HttpClientImpl(properties);
    }

    public void invoke(Tuple6<String, String, String, String, String, String> value) {

        try {
            //写入OpenTSDB
            MetricBuilder builder = MetricBuilder.getInstance();
            String metric = value.f2;
            String host = value.f1;
            String id = value.f3;
            long time = Long.parseLong(value.f4);
            //转为保留两位小数
            java.text.DecimalFormat df = new java.text.DecimalFormat("#.##");
            double result = Double.parseDouble(df.format(Double.parseDouble(value.f5)));
            if (value.f0.contains("h3c")) {
                long l = System.currentTimeMillis();
                String metircs = metric + "-" + host + "-" + id;
                //华三交换机存储opentsdb策略
                builder.addMetric(metircs)
                        .setDataPoint(time, result)
                        .addTag("type", "h3c_swtich")
                        .addTag("host", host);
            } else if (value.f0.contains("zx")) {
                String metircs = metric + "-" + host + "-" + id;
                long l = System.currentTimeMillis();
                //华三交换机存储opentsdb策略
                builder.addMetric(metircs)
                        .setDataPoint(time, result)
                        .addTag("type", "zx_swtich")
                        .addTag("host", host);
            } else if (value.f0.contains("dpi")) {
                String metircs = metric + "-" + host + "-" + id;
                long l = System.currentTimeMillis();
                //华三交换机存储opentsdb策略
                builder.addMetric(metircs)
                        .setDataPoint(time, result)
                        .addTag("type", "dpi")
                        .addTag("host", host);
            } else if (RG_AS224GT.contains(metric)) {
                builder.addMetric(metric + "-" + host)
                        .setDataPoint(time, result);
//                        .addTag("type", "RG_AS224GT");
            } else {
                builder.addMetric(metric + "-" + host)
                        .setDataPoint(time, result)
                        .addTag("host", host);
            }
            Response response1 = httpClient.pushMetrics(builder, ExpectResponse.SUMMARY);
        } catch (Exception e) {
            e.printStackTrace();
        }

        /**
         String metric  = value.f2;
         String host = value.f1;
         Long time = Long.parseLong(value.f4);
         Double result = value.f5;
         System.out.println(metric+"$"+host+"$"+time+"$"+result);
         */
    }


}
