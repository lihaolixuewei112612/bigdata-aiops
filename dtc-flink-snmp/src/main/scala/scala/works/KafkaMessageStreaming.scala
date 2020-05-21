//package com.dtc.analytic.scala.works
//
//import java.text.SimpleDateFormat
//import java.util.{Date, Properties}
//
//import com.dtc.analytic.scala.common.{DtcConf, LevelEnum}
//import org.apache.flink.api.common.functions.MapFunction
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.watermark.Watermark
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
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
//    var window = inputMap.keyBy(0)
//      .timeWindow(Time.seconds(10), Time.seconds(5))
//    inputMap.print()
//    env.execute("StreamingWindowWatermarkScala")
//  }
//
//
//}
//
//class MyMapFunction extends MapFunction[String, Tuple2[String, String]] {
//  override def map(line: String): Tuple2[String, String] = {
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
//      return (event(3).trim, message)
//    } else {
//      val event = line.split("\\$\\$")
//      message += "{" + "\"time\"" + ":" + "\"" + event(0).trim + "\"" + "," + "\"device\"" + ":" + "\"" + event(1).trim +
//        "\"" + "," + "\"" + "level" + "\"" + ":" + "\"" + event(2).trim + "\"" + "," + "\"" + "hostname" + "\"" + ":" +
//        "\"" + event(3).trim + "\"" + "," + "\"" + "message" + "\"" + ":" + "\"" + event(4).trim + "\"" + "}"
//      var time1 = event(0).replace("T", " ").split("\\+")(0).replace("-", "/")
//      var time2 = new Date(time1).getTime
//      return (event(3).trim, message)
//    }
//
//  }
//}
