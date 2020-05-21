//package com.dtc.analytic.scala.demo
//
//import java.lang
//import java.text.SimpleDateFormat
//import java.util.{Date, Properties}
//
//import com.dtc.analytic.scala.common.{DtcConf, LevelEnum, Utils}
//import com.dtc.analytic.scala.dtcexpection.DtcException
//import org.apache.flink.api.common.functions.{MapFunction, RuntimeContext}
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.java.tuple.Tuple
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
//import org.apache.flink.streaming.api.functions.windowing.WindowFunction
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.watermark.Watermark
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
//import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
//import org.apache.flink.util.Collector
//import org.apache.http.HttpHost
//import org.elasticsearch.action.index.IndexRequest
//import org.elasticsearch.client.Requests
//import org.elasticsearch.common.xcontent.XContentType
//
///**
//  * Created on 2019-06-06
//  *
//  * @author :hao.li
//  */
//object KafkaMessageStreaming {
//  def main(args: Array[String]): Unit = {
//    DtcConf.setup()
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)
//    val conf = DtcConf.getConf()
//    val level = conf.get("log.level")
//    val lev: Int = LevelEnum.getIndex(level)
//
//    val brokerList = conf.get("flink.kafka.broker.list")
//    val topic = conf.get("flink.kafka.topic")
//    val groupId = conf.get("flink.kafka.groupid")
//    val prop = new Properties()
//    prop.setProperty("bootstrap.servers", brokerList)
//    prop.setProperty("group.id", groupId)
//    prop.setProperty("topic", topic)
//    val myConsumer = new FlinkKafkaConsumer09[String](topic, new SimpleStringSchema(), prop)
//    val waterMarkStream = myConsumer.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String)] {
//      var currentMaxTimestamp = 0L
//      var maxOutOfOrderness = 10000L // 最大允许的乱序时间是10s
//      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//
//      override def extractTimestamp(element: (String), previousElementTimestamp: Long) = {
//        val event = element.split("\\$\\$")
//        var time1 = event(0).replace("T", " ").split("\\+")(0).replace("-", "/")
//        var time2 = new Date(time1).getTime
//        currentMaxTimestamp = Math.max(time2, currentMaxTimestamp)
//        time2
//      }
//
//      override def getCurrentWatermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
//
//    })
//    val text = env.addSource(myConsumer)
//    var inputMap = text.map(new MyMapFunction)
//    var window = inputMap
//      .timeWindow(Time.seconds(10), Time.seconds(5))
//    inputMap.print()
//    env.execute("StreamingWindowWatermarkScala")
//  }
//
//
//}
//
//class MyMapFunction extends MapFunction[String, String] {
//  override def map(line: String): String= {
//    var message = ""
//    if (line.contains("$DTC$")) {
//      val splitDtc: Array[String] = line.split("\\$DTC\\$")
//      val event = splitDtc(0).split("\\$\\$")
//      message += "{" + "\"time\"" + ":" + "\"" + event(0).trim + "\"" + "," + "\"device\"" + ":" + "\"" + event(1).trim +
//        "\"" + "," + "\"" + "level" + "\"" + ":" + "\"" + event(2).trim + "\"" + "," + "\"" + "hostname" + "\"" + ":" +
//        "\"" + event(3).trim + "\"" + "," + "\"" + "message" + "\"" + ":" + "\"" + event(4).trim + "\""
//      var str = ""
//      for (i <- 1 until splitDtc.length) {
//        str = splitDtc(i).trim + "\n"
//      }
//      message += "\"" + "cause" + "\"" + ":" + "\"" + str.trim + "\"" + "}"
//      var time1 = event(0).replace("T", " ").split("\\+")(0).replace("-", "/")
//      var time2 = new Date(time1).getTime
//      return message
//    } else {
//      val event = line.split("\\$\\$")
//      message += "{" + "\"time\"" + ":" + "\"" + event(0).trim + "\"" + "," + "\"device\"" + ":" + "\"" + event(1).trim +
//        "\"" + "," + "\"" + "level" + "\"" + ":" + "\"" + event(2).trim + "\"" + "," + "\"" + "hostname" + "\"" + ":" +
//        "\"" + event(3).trim + "\"" + "," + "\"" + "message" + "\"" + ":" + "\"" + event(4).trim + "\"" + "}"
//      var time1 = event(0).replace("T", " ").split("\\+")(0).replace("-", "/")
//      var time2 = new Date(time1).getTime
//      return  message
//    }
//
//  }
//}
//
//
