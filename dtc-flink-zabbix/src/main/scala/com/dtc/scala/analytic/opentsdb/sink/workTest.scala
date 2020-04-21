package com.dtc.scala.analytic.opentsdb.sink

import java.io.IOException

import org.opentsdb.client.{ExpectResponse, HttpClientImpl}
import org.opentsdb.client.builder.MetricBuilder
import org.opentsdb.client.response.Response

/**
  * Created on 2019-08-14
  *
  * @author :hao.li
  */
object workTest {
  def main(args: Array[String]): Unit = {
    val client = new HttpClientImpl("http://10.3.0.170:4242")

    val builder = MetricBuilder.getInstance

    builder.addMetric("metric5").setDataPoint(300L).addTag("tag1", "tab1value").addTag("tag2", "tab2value")

    //		builder.addMetric("metric2").setDataPoint(232.34)
    //				.addTag("tag3", "tab3value");

    try {
      val response = client.pushMetrics(builder, ExpectResponse.SUMMARY)
      System.out.println(response)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }


}
