package com.dtc.scala.analytic.lihao

import java.util.concurrent.TimeUnit

import com.dtc.scala.analytic.opentsdb.sink.OpentsdbSinkOne
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector

/**
  * Created on 2019-08-12
  *
  * @author :hao.li
  */

object work {
  def main(args: Array[String]): Unit = {
    //    val params = ParameterTool.fromArgs(args)
    //    checkParams(params)
    //    val windowSizeMillis = params.getRequired("window-size-millis").toLong
    //    val windowSlideMillis = params.getRequired("window-slide-millis").toLong
    val windowSizeMillis = 5000
    val windowSlideMillis = 1000

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.enableCheckpointing(60000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.setParallelism(1)
    val stream: DataStream[String] = env.addSource(new SimulatedEventSource)
//    val stream = env.readTextFile("/Users/lixuewei/workspace/DTC/workspace/dtc_bigdata/dtc-flink/src/main/resources/abc.txt")
    //    stream.print()
    /**
      * (a,23822888381703	2724-12-02 04:59:41	70eb64cf-16bd-41f1-90a6-646d60dc2690	PURCHASE)
      * 1> (c,23822892247033	2724-12-02 06:04:07	b3993786-e049-4848-a9b4-63f36bebdb43	UNINSTALL)
      * 3> (b,23822893521052	2724-12-02 06:25:21	f1d6010d-09ee-41d1-a76b-8cfc2320a2fb	PURCHASE)
      * 2> (d,23822900263023	2724-12-02 08:17:43	e9ff07e7-2568-4cac-adb7-590570dfa8c5	CLOSE)
      */

    var inputMap = stream.map(new MyMapFunction
//      line => {
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
//    }
    )
    val result = inputMap.keyBy(0)
    //    val result1 = result.keyBy(1)
    //    val result2 = result1.keyBy(2)
    //    result.print()
    val result3 = result
      .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS), Time.of(windowSlideMillis, TimeUnit.MILLISECONDS))
      .process(new MyProcessWindowFunction)
//      .map(t => {
//        val key = t._1
//        val count = t._2
//        val windowStartTime = t._3
//        val windowEndTime = t._4
//        val channel = t._5
//        val behaviorType = t._6
//        Seq(key, count, windowStartTime, windowEndTime, channel,behaviorType)
//      })
//    result3.print()
    result3.addSink(new OpentsdbSinkOne)

    // create a Kafka producer for Kafka 0.9.x
    //    val kafkaProducer = new FlinkKafkaProducer09(
    //      params.getRequired("window-result-topic"),
    //      new SimpleStringSchema, params.getProperties
    //    )

    //    val result = stream
    //      .map(t => {
    //        val channel = t._1
    //        val eventFields = t._2.split("\t")
    //        val behaviorType = eventFields(3)
    //        ((channel, behaviorType), 1L)
    //      })
    //      .keyBy(0)
    //    val result1 = result.timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS), Time.of(windowSlideMillis, TimeUnit.MILLISECONDS))
    //      .process(new MyReduceWindowFunction)
    //      .map(t => {
    //        val key = t._1
    //        val count = t._2
    //        val windowStartTime = key._1
    //        val windowEndTime = key._2
    //        val channel = key._3
    //        val behaviorType = key._4
    //        Seq(windowStartTime, windowEndTime, channel, behaviorType, count).mkString("\t")
    //      })
    //    result1.print()

    env.execute(getClass.getSimpleName)
  }

}

class MyMapFunction extends MapFunction[String, ((String, String, String), String, String, Double)] {
  override def map(line: String): ((String, String, String), String, String, Double) = {
    //    var message = ()
    val jsonParser = new JSONParser()
    val json = jsonParser.parse(line).asInstanceOf[JSONObject]
    val name = json.get("code").toString.split("\\|")
    val system_name = name(0).trim + "_" + name(1).trim
    val ZB_Name = system_name + "_" + name(2).trim + "_" + name(3).trim
    val ZB_Code = name(4).trim
    val time = json.get("time").toString
    val value = json.get("value").toString.toDouble
    val host = json.get("host").toString.trim
    val message = ((system_name, host, ZB_Name), ZB_Code, time, value)
    return message
  }
}

class MyProcessWindowFunction extends ProcessWindowFunction[((String, String, String), String, String, Double), (String, String, String, String, String, Double), Tuple, TimeWindow] {
  override def process(key: Tuple, context: Context, elements: Iterable[((String, String, String), String, String, Double)], out: Collector[(String, String, String, String, String, Double)]): Unit = {
    for (str <- elements.groupBy(_._1)) {
      var map: scala.collection.mutable.Map[String, Double] = scala.collection.mutable.Map()
      val myKey = str._1
      val myValue = str._2
      for (elem <- myValue) {
        import util.control.Breaks._
        breakable {
          if (myKey._3 == "101_101_101_101_105") {
            if (elem._2.equals("106")) {
              map += ("106" -> elem._4)
              if (map.contains("107")) {
                val result = map.getOrElse("107", 0.0) / map.getOrElse("106", 1.0)
                out.collect((myKey._1, myKey._2, myKey._3, "000", elem._3, result))
              } else {
                break()
              }
            }
            if (elem._2.equals("107")) {
              map += ("107" -> elem._4)
              if (map.contains("106")) {
                val result = map.getOrElse("107", 0.0) / map.getOrElse("106", 1.0)
                out.collect((myKey._1, myKey._2, myKey._3, "000", elem._3, result))
              } else {
                break()
              }
            }
          }
        }
        out.collect((myKey._1, myKey._2, myKey._3, elem._2, elem._3, elem._4))
      }
    }
  }
}
