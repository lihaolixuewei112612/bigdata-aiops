//package com.dtc.scala.analytic.works
//
//import java.util.concurrent.TimeUnit
//
//import com.dtc.scala.analytic.common.DtcConf
//import net.minidev.json.JSONObject
//import net.minidev.json.parser.JSONParser
//import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
//import org.apache.flink.api.java.tuple.Tuple
//import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
//import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
//import org.apache.flink.util.Collector
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
//    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    //    env.setParallelism(1)
//    //    val conf = DtcConf.getConf()
//    //    val level = conf.get("log.level")
//    //    val lev: Int = LevelEnum.getIndex(level)
//
//    //
//    //    val brokerList = conf.get("flink.kafka.broker.list")
//    //    val topic = conf.get("flink.kafka.topic")
//    //    val groupId = conf.get("flink.kafka.groupid")
//    //    val prop = new Properties()
//    //    prop.setProperty("bootstrap.servers", brokerList)
//    //    prop.setProperty("group.id", groupId)
//    //    prop.setProperty("topic", topic)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    env.enableCheckpointing(60000)
//    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    env.setParallelism(1)
//    val windowSizeMillis = 5
//    val windowSlideMillis = 1
//    val flieStream = env.readTextFile("/Users/lixuewei/workspace/DTC/workspace/dtc_bigdata/dtc-flink/src/main/resources/abc.txt")
//
////        val myConsumer = new FlinkKafkaConsumer09[String](topic, new SimpleStringSchema(), prop)
//    //    val waterMarkStream = myConsumer.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String)] {
//    //      var currentMaxTimestamp = 0L
//    //      var maxOutOfOrderness = 10000L // 最大允许的乱序时间是10s
//    //      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    //
//    //      override def extractTimestamp(element: (String), previousElementTimestamp: Long) = {
//    //        val event = element.split("\\$\\$")
//    //        var time1 = event(0).replace("T", " ").split("\\+")(0).replace("-", "/")
//    //        var time2 = new Date(time1).getTime
//    //        currentMaxTimestamp = Math.max(time2, currentMaxTimestamp)
//    //        time2
//    //      }
//    //
//    //      override def getCurrentWatermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
//    //
//    //    })
//    //    val text = env.addSource(flieStream)
//    //        var inputMap = flieStream.flatMap(new myFlatMapFunction).keyBy("System_name").keyBy("Host").sum("value")
//    var inputMap = flieStream.map(line => {
//      val jsonParser = new JSONParser()
//      val json = jsonParser.parse(line).asInstanceOf[JSONObject]
//      val name = json.get("code").toString.split("\\|")
//      val system_name = name(0).trim + "_" + name(1).trim
//      val ZB_Name = system_name + "_" + name(2).trim + "_" + name(3).trim
//      val ZB_Code = name(4).trim
//      val time = json.get("time").toString
//      val value = json.get("value").toString.toDouble
//      val host = json.get("host").toString.trim
//      ((system_name, host, ZB_Name), ZB_Code, time, value)
//    })
//    val result = inputMap.keyBy(0)
//    //    val result1 = result.keyBy(1)
//    //    val result2 = result1.keyBy(2)
//    result.print()
//    val result3 = result
//      .timeWindow(Time.of(windowSizeMillis, TimeUnit.SECONDS), Time.of(windowSlideMillis, TimeUnit.SECONDS))
//      .process(new MyProcessWindowFunction)
//    result3.print()
//    env.execute("StreamingWindowWatermarkScala")
//  }
//
//  class myFlatMapFunction extends FlatMapFunction[String, (String, String, String, String, String, Double)] {
//    override def flatMap(t: String, collector: Collector[(String, String, String, String, String, Double)]): Unit = {
//      var message: (String, String, String, String, String, Double) = null
//      val jsonParser = new JSONParser()
//      val json = jsonParser.parse(t).asInstanceOf[JSONObject]
//      val name = json.get("code").toString.split("\\|")
//      if (name.length == 5) {
//        val system_name = name(0).trim + "_" + name(1).trim
//        val ZB_Name = system_name + "_" + name(2).trim + "_" + name(3).trim
//        val ZB_Code = name(4).trim
//        val time = json.get("time").toString
//        val value = json.get("value").toString.toDouble
//        val host = json.get("host").toString.trim
//        message = (system_name, host, ZB_Name, ZB_Code, time, value)
//        collector.collect(message)
//      }
//    }
//  }
//
//}
//
//class MyMapFunction extends MapFunction[String, ((String, String, String), String, String, Double)] {
//  override def map(line: String): ((String, String, String), String, String, Double) = {
//    //    var message = ()
//    val jsonParser = new JSONParser()
//    val json = jsonParser.parse(line).asInstanceOf[JSONObject]
//    val name = json.get("code").toString.split("\\|")
//    val system_name = name(0).trim + "_" + name(1).trim
//    val ZB_Name = system_name + "_" + name(2).trim + "_" + name(3).trim
//    val ZB_Code = name(4).trim
//    val time = json.get("time").toString
//    val value = json.get("value").toString.toDouble
//    val host = json.get("host").toString.trim
//    val message = ((system_name, host, ZB_Name), ZB_Code, time, value)
//    return message
//  }
//
//  //    val time = json.get("timestamp").toString
//  //    val value = json.get("value").toString.toDouble
//  //    val lable = jsonParser.parse(json.get("labels").toString).asInstanceOf[JSONObject]
//  //    val lable_ip = lable.get("instance").toString
//  //    val result = (name, lable_ip, time, value)
//}
//
//class MyProcessWindowFunction extends ProcessWindowFunction[((String, String, String), String, String, Double), ((String, String, String), Double), Tuple, TimeWindow] {
//  override def process(key: Tuple, context: Context, elements: Iterable[((String, String, String), String, String, Double)], out: Collector[((String, String, String), Double)]): Unit = {
//    //    var map: scala.collection.mutable.Map[String, Double] = scala.collection.mutable.Map()
//    //    for (str <- elements) {
//    //      if (str._1._1 == "101_101_101_101_105") {
//    //        if (str._3.equals("106")) {
//    //          map += ("106" -> str._4)
//    //          if (map.contains("107")) {
//    //            val result = map.getOrElse("107", 0.0) / map.getOrElse("106", 1.0)
//    //            out.collect((str._1._1, str._1._2, str._1._3, str._2 + "000", str._3, result))
//    //          } else {
//    //            out.collect((str._1._1, str._1._2, str._1._3, str._2 + "000", str._3, str._4))
//    //          }
//    //        }
//    //        if (str._3.equals("107")) {
//    //          map += ("107" -> str._4)
//    //          if (map.contains("106")) {
//    //            val result = map.getOrElse("107", 0.0) / map.getOrElse("106", 1.0)
//    //            out.collect((str._1._1, str._1._2, str._1._3, str._2 + "000", str._3, result))
//    //          } else {
//    //            out.collect((str._1._1, str._1._2, str._1._3, str._2 + "000", str._3, str._4))
//    //          }
//    //        }
//    //      }
//    //      out.collect((str._1._1, str._1._2, str._1._3, str._2 + "000", str._3, str._4))
//    //    }
//    for (str <- elements.groupBy(_._1)) {
//      val a = str._1
//      var b = str._2
//      var count = 0D
//      for (elem <- b) {
//        count += elem._4
//      }
//      out.collect((a, count))
//    }
//  }
//}
//
//
//class MyReduceWindowFunction
//  extends ProcessWindowFunction[(String, String, String, String, String, Double), (String, String, String), Tuple, TimeWindow] {
//
//  override def process(key: Tuple, context: Context,
//                       elements: Iterable[(String, String, String, String, String, Double)],
//                       collector: Collector[(String, String, String)]): Unit = {
//    for (str <- elements) {
//      val a = str._1
//      val b = str._2
//      val c = str._3
//      collector.collect((a, b, c))
//    }
//  }
//}
//
////class OneMapFunction extends MapFunction[String,JSONObject]{
////  override def map(t: String): JSONObject = {
////    val jsonParser = new JSONParser()
////    val json = jsonParser.parse(t).asInstanceOf[JSONObject]
////    return json
////  }
////}
////
////case class PDataStruct() {
////  var system_name: String = null
////  var lable_ip:String=null
////  var zhibiao_name:String= null
////  var time:String=null
////  var value = 0.0
////
//////  def this(system_name: String,lable_ip:String,zhibiao_name:String,time:String, value: Double) {
//////    this()
//////    this.system_name = system_name
//////    this.lable_ip = lable_ip
//////    this.zhibiao_name=zhibiao_name
//////    this.time = time
//////    this.value=value
//////  }
//////
//////  override def toString: String = "PDataStruct{" + "system_name='" + system_name + '\'' + ", lable_ip=" + lable_ip + '}'
////}
