package com.dtc.scala.analytic.opentsdb.sink

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient

/**
  * Created on 2019-08-14
  *
  * @author :hao.li
  */
object test {
  def main(args: Array[String]): Unit = {
    val abc =OpenTSDBMetric(123L,"lihao",20.0f,"abc")
    val str = "http://10.3.0.20:4242/api/put"
    println("------------------")
    postMetric(str,abc)
    println("333333333333333333")
  }

  def postMetric(url: String,
                 metric: OpenTSDBMetric) = {

    val client = new DefaultHttpClient
    val post = new HttpPost(url)

    val input = new StringEntity("{" +
      "\"metric\": \"" + metric.name + "\"," +
      "\"timestamp\": " + metric.timestamp + "," +
      "\"value\": " + metric.value + "," +
      "\"tags\": { " + metric.tags + "} }");
//    input.setContentType("application/json");

    post.setHeader("Content-type", "application/json")
    post.setEntity(input);

    val response = client.execute(post)
    println(response.getStatusLine.getStatusCode)

    //response.getStatusLine().getStatusCode()
  }

}
case class OpenTSDBMetric(var timestamp: Long, var name: String, var value: Float, var tags: String) {

}
