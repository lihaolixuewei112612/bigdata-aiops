package com.dtc.scala.analytic.opentsdb.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.opentsdb.client.{ExpectResponse, HttpClientImpl, PoolingHttpClient}
import org.opentsdb.client.builder.MetricBuilder

/**
  * Created on 2019-08-13
  *
  * @author :hao.li
  */
class OpentsdbSinkOne extends RichSinkFunction[(String, String, String, String, String, Double)] {
  var client: HttpClientImpl = _
  var clientest: PoolingHttpClient = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    clientest = new PoolingHttpClient
    client = new HttpClientImpl("http://10.3.0.170:4242")

  }

  override def invoke(value: (String, String, String, String, String, Double)): Unit = {
    val builder = MetricBuilder.getInstance()
    builder.addMetric("dtc_test").setDataPoint(value._6).addTag("time", value._5).addTag("lihao", "test")
    val name = clientest.doPost("http://10.3.0.170:4242/api/put/?details", builder.build())
    println(name)
    //    client.pushMetrics(builder)
  }

}
