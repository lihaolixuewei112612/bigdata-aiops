//package com.dtc.scala.analytic.works
//
//import java.text.SimpleDateFormat
//import java.util.{Date, Properties}
//
//import com.dtc.scala.analytic.common.{CountUtils, DtcConf, LevelEnum, Utils}
//import com.dtc.scala.analytic.dtcexpection.DtcException
//import org.apache.flink.api.common.functions.{MapFunction, RuntimeContext}
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.watermark.Watermark
//import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
//import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
//import org.apache.http.HttpHost
//import org.elasticsearch.action.index.IndexRequest
//import org.elasticsearch.client.Requests
//import org.elasticsearch.common.xcontent.XContentType
//import org.slf4j.{Logger, LoggerFactory}
//
///**
//  * Created on 2019-05-27
//  *
//  * @author :hao.li
//  */
//object StreamingFlinkScala {
//  def logger: Logger = LoggerFactory.getLogger(StreamingFlinkScala.getClass)
//
//  def main(args: Array[String]): Unit = {
//    DtcConf.setup()
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    import org.apache.flink.streaming.api.CheckpointingMode
//    import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
//    env.enableCheckpointing(60000)
//    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    env.setParallelism(1)
//    val conf = DtcConf.getConf()
//    val level: Int = LevelEnum.getIndex(conf.get("log.level"))
//    val brokerList = conf.get("flink.kafka.broker.list")
//    val topic = conf.get("flink.kafka.topic")
//    val groupId = conf.get("flink.kafka.groupid")
//
//    val prop = new Properties()
//    prop.setProperty("bootstrap.servers", brokerList)
//    prop.setProperty("group.id", groupId)
//    prop.setProperty("topic", topic)
//    val myConsumer = new FlinkKafkaConsumer09[String](topic, new SimpleStringSchema(), prop)
//    val waterMarkStream = myConsumer.assignTimestampsAndWatermarks(new DTCPeriodicWatermarks)
//    val text = env.addSource(waterMarkStream)
//
//    val inputMap = text.map(new DTCMapFunction).filter(!_.contains("null"))
//    var window: DataStream[String] = null
//    if (1 <= level && level < 2) {
//      window = inputMap
//        .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//        .fold("") { (acc, v) => v }
//    } else if (2 <= level && level < 3) {
//      window = inputMap
//        .filter(!_.contains("info"))
//        .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//        .fold("") { (acc, v) => v }
//    } else if (3 <= level && level < 4) {
//      window = inputMap
//        .filter { x => (!x.contains("info") || (!x.contains("debug"))) }
//        .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//        .fold("") { (acc, v) => v }
//    } else if (4 <= level && level < 5) {
//      window = inputMap
//        .filter { x => (!x.contains("info") || (!x.contains("debug")) || (!x.contains("notice"))) }
//        .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//        .fold("") { (acc, v) => v }
//    } else if (5 <= level && level < 6) {
//      window = inputMap
//        .filter { x =>
//          (!x.contains("info") || (!x.contains("debug")) || (!x.contains("notice"))
//            || (!x.contains("warning")) || (!x.contains("warn")))
//        }
//        .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//        .fold("") { (acc, v) => v }
//    }
//    else if (6 <= level && level < 7) {
//      window = inputMap
//        .filter { x =>
//          (!x.contains("info") || (!x.contains("debug")) || (!x.contains("notice"))
//            || (!x.contains("warning")) || (!x.contains("warn")) || (!x.contains("err")))
//        }
//        .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//        .fold("") { (acc, v) => v }
//    }
//    else if (7 <= level && level < 8) {
//      window = inputMap
//        .filter { x =>
//          (!x.contains("info") || (!x.contains("debug")) || (!x.contains("notice"))
//            || (!x.contains("warning")) || (!x.contains("warn")) || (!x.contains("err")) || (!x.contains("crit")))
//        }
//        .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//        .fold("") { (acc, v) => v }
//    }
//    else {
//      window = inputMap
//        .filter { x =>
//          (!x.contains("info") || (!x.contains("debug")) || (!x.contains("notice"))
//            || (!x.contains("warning")) || (!x.contains("warn")) || (!x.contains("err")) || (!x.contains("crit"))
//            || (!x.contains("alert")))
//        }
//        .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//        .fold("") { (acc, v) => v }
//    }
//    val httpHosts = new java.util.ArrayList[HttpHost]
//    val es_host = conf.get("dtc.es.nodes")
//    if (Utils.isEmpty(es_host)) {
//      throw new DtcException("Es_Host is null!")
//    }
//    val host_es = es_host.split(",")
//    for (x <- host_es) {
//      val ip = x.split(":")(0)
//      val host = x.split(":")(1)
//      httpHosts.add(new HttpHost(ip, host.toInt, "http"))
//    }
//
//    val es_index = conf.get("dtc.es.flink.index.name")
//
//    val es_type = conf.get("dtc.es.flink.type.name")
//    if (Utils.isEmpty(es_index) || Utils.isEmpty(es_type)) {
//      throw new DtcException("Es_index or type is null!")
//    }
//    var esSink = new ElasticsearchSink.Builder[String](httpHosts, new ElasticsearchSinkFunction[String] {
//      def createIndexRequest(element: String): IndexRequest = {
//        return Requests.indexRequest()
//          .index(es_index)
//          .`type`(es_type)
//          .source(element, XContentType.JSON)
//      }
//
//      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
//        //          val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`(indexType).source(element,XContentType.JSON)
//        //                  indexer.add(indexRequest)
//        indexer.add(createIndexRequest(element))
//      }
//    }
//    )
//
//    esSink.setBulkFlushMaxActions(1)
//    window.addSink(esSink.build())
//    env.execute("StreamingWindowWatermarkScala")
//  }
//
//
//  def tranTimeToString(timestamp: String): String = {
//    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val time = fm.format(new Date(timestamp.toLong))
//    time
//  }
//
//}
//
//class DTCMapFunction extends MapFunction[String, String] {
//  override def map(line: String): String = {
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
//      CountUtils.incrementEventRightCount
//      return message
//    } else if (line.contains("$$")) {
//      val event = line.split("\\$\\$")
//      message += "{" + "\"time\"" + ":" + "\"" + event(0).trim + "\"" + "," + "\"device\"" + ":" + "\"" + event(1).trim +
//        "\"" + "," + "\"" + "level" + "\"" + ":" + "\"" + event(2).trim + "\"" + "," + "\"" + "hostname" + "\"" + ":" +
//        "\"" + event(3).trim + "\"" + "," + "\"" + "message" + "\"" + ":" + "\"" + event(4).trim + "\"" + "}"
//      var time1 = event(0).replace("T", " ").split("\\+")(0).replace("-", "/")
//      var time2 = new Date(time1).getTime
//      CountUtils.incrementEventRightCount
//      return message
//    } else {
//      message += "{" + "null" + "}"
//      CountUtils.incrementEventErrorCount
//      return message
//    }
//
//  }
//}
//
////  def getEsSink(indexName: String): ElasticsearchSink[String] = {
////    //new接口---> 要实现一个方法
////    val esSinkFunc: ElasticsearchSinkFunction[String] = new ElasticsearchSinkFunction[String] {
////      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
////        val json = new java.util.HashMap[String, String]
////        json.put("data", element)
////        val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(json)
////        indexer.add(indexRequest)
////      }
////    }
////    val esSinkBuilder = new ElasticsearchSink.Builder[String](hostList, esSinkFunc)
////    esSinkBuilder.setBulkFlushMaxActions(10)
////    val esSink: ElasticsearchSink[String] = esSinkBuilder.build()
////    esSink
////  }
//class DTCPeriodicWatermarks extends AssignerWithPeriodicWatermarks[String] {
//  var currentMaxTimestamp = 0L
//  var maxOutOfOrderness = 10000L // 最大允许的乱序时间是10s
//  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//
//  override def extractTimestamp(element: String, previousElementTimestamp: Long) = {
//    val event = element.split("\\$\\$")
//    val time1 = event(0).replace("T", " ").split("\\+")(0).replace("-", "/")
//    val time2 = new Date(time1).getTime
//    currentMaxTimestamp = Math.max(time2, currentMaxTimestamp)
//    time2
//  }
//
//  override def getCurrentWatermark(): Watermark = {
//    return new Watermark(currentMaxTimestamp - maxOutOfOrderness)
//  }
//
//}
//
//
//
//
//
//
//
//
//
//
