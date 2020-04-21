package com.dtc.java.analytic.V2.sink.opentsdb;

import com.dtc.java.analytic.V2.common.model.DataStruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.opentsdb.client.ExpectResponse;
import org.opentsdb.client.HttpClientImpl;
import org.opentsdb.client.builder.MetricBuilder;
import org.opentsdb.client.response.Response;

/**
 * Created on 2020-02-25
 *
 * @author :hao.li
 */
@Slf4j
public class PSinkToOpentsdb extends RichSinkFunction<DataStruct> {

    String properties;
    HttpClientImpl httpClient;

    public PSinkToOpentsdb(String prop) {
        this.properties = prop;
    }

    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        httpClient = new HttpClientImpl(properties);
    }

    public void invoke(DataStruct value) {

        try {
            //写入OpenTSDB
            MetricBuilder builder = MetricBuilder.getInstance();
            String metric = value.getZbFourName();
            String host = value.getHost();
            String id = value.getZbLastCode();
            long time = Long.parseLong(value.getTime());
            //转为保留两位小数
            java.text.DecimalFormat df = new java.text.DecimalFormat("#.##");
            double result = Double.parseDouble(df.format(Double.parseDouble(value.getValue())));
            if (value.getSystem_name().contains("h3c")) {
                long l = System.currentTimeMillis();
                String metircs = metric + "-" + host + "-" + id;
                //华三交换机存储opentsdb策略
                builder.addMetric(metircs)
                        .setDataPoint(time, result)
                        .addTag("type", "h3c_swtich")
                        .addTag("host", host);
            } else if (value.getSystem_name().contains("zx")) {
                String metircs = metric + "-" + host + "-" + id;
                long l = System.currentTimeMillis();
                //华三交换机存储opentsdb策略
                builder.addMetric(metircs)
                        .setDataPoint(time, result)
                        .addTag("type", "zx_swtich")
                        .addTag("host", host);
            } else if (value.getSystem_name().contains("dpi")) {
                String metircs = metric + "-" + host + "-" + id;
                long l = System.currentTimeMillis();
                //华三交换机存储opentsdb策略
                builder.addMetric(metircs)
                        .setDataPoint(time, result)
                        .addTag("type", "dpi")
                        .addTag("host", host);
            }else if(value.getSystem_name().contains("win")){
                if(id.isEmpty()){
                    String metircs = metric + "-" + host;
                    builder.addMetric(metircs)
                            .setDataPoint(time, result)
                            .addTag("type", "win")
                            .addTag("host", host);
                }else {
                    String metircs = metric + "-" + id+ "-" + host ;
                    builder.addMetric(metircs)
                            .setDataPoint(time, result)
                            .addTag("type", "win")
                            .addTag("lastcode", id)
                            .addTag("host", host);
                }
            } else {
                builder.addMetric(metric + "-" + host)
                        .setDataPoint(time, result)
                        .addTag("host", host);
            }
            Response response1 = httpClient.pushMetrics(builder, ExpectResponse.SUMMARY);
            boolean success = response1.isSuccess();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
