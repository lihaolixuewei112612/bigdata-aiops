//package com.dtc.analytic.scala.works
//
//import java.text.SimpleDateFormat
//import java.util.{Date, Properties}
//
//import com.dtc.analytic.scala.Main
//import com.dtc.analytic.scala.common.{DtcConf, LevelEnum, Utils}
//import com.dtc.analytic.scala.dtcexpection.DtcException
//import org.apache.flink.api.common.functions.{MapFunction, RuntimeContext}
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.watermark.Watermark
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
//import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
//import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
//import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
//import org.apache.http.HttpHost
//import org.codehaus.jettison.json.JSONObject
//import org.elasticsearch.action.index.IndexRequest
//import org.elasticsearch.client.Requests
//import org.elasticsearch.common.xcontent.XContentType
//import org.mortbay.util.ajax.JSON
//import org.slf4j.{Logger, LoggerFactory}
//
///**
//  * Created on 2019-05-27
//  *
//  * @author :hao.li
//  */
//object StreamingWindowWatermarkScala {
//  def logger: Logger = LoggerFactory.getLogger(StreamingWindowWatermarkScala.getClass)
//
//  def main(args: Array[String]): Unit = {
//    try {
//      DtcConf.setup()
//    }
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
//    prop.setProperty("topic",topic)
//    val myConsumer = new FlinkKafkaConsumer09[String](topic, new SimpleStringSchema(), prop)
//    val text = env.addSource(myConsumer)
//
//    //    val text = env.readTextFile("conf/test.log")
//    var inputMap = text.map(new MyMapFunction)
//
//
//
//
//    val waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
//      var currentMaxTimestamp = 0L
//      var maxOutOfOrderness = 10000L // 最大允许的乱序时间是10s
//
//      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//
//      override def getCurrentWatermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
//
//      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long) = {
//        val timestamp = element._2
//        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
//        val id = Thread.currentThread().getId
//        //        println("currentThreadId:" + id + ",key:" + element._1 + ",eventtime:[" + element._2 + "|" + sdf.format(element._2) + "],currentMaxTimestamp:[" + currentMaxTimestamp + "|" + sdf.format(currentMaxTimestamp) + "],watermark:[" + getCurrentWatermark().getTimestamp + "|" + sdf.format(getCurrentWatermark().getTimestamp) + "]")
//        currentMaxTimestamp
//      }
//    })
//
//    //    var window = waterMarkStream.map(x => (x._1, 1)).filter(x => x._1.contains("info") || x._1.contains("debug") ||
//    //      x._1.contains("notice") || x._1.contains("warning") || x._1.contains("warn") || x._1.contains("err") ||
//    //      x._1.contains("crit") || x._1.contains("alert") || x._1.contains("emerg") || x._1.contains("panic"))
//    //      .keyBy(0)
//    //      .timeWindow(Time.seconds(10), Time.seconds(5))
//    //      .sum(1)
//    //      .map(x => x._1)
//
//
//    var window: DataStream[String] = null
//    if (1 <= lev && lev < 2) {
//      window = waterMarkStream.map(x => (x._1, 1)).filter(x => x._1.contains("info") || x._1.contains("debug") ||
//        x._1.contains("notice") || x._1.contains("warning") || x._1.contains("warn") || x._1.contains("err") ||
//        x._1.contains("crit") || x._1.contains("alert") || x._1.contains("emerg") || x._1.contains("panic"))
//        .keyBy(0)
//        .timeWindow(Time.seconds(10), Time.seconds(5))
//        .sum(1)
//        .map(x => x._1)
//    }
//    else if (2 <= lev && lev < 3) {
//      window = waterMarkStream.map(x => (x._1, 1)).filter(x => x._1.contains("debug") ||
//        x._1.contains("notice") || x._1.contains("warning") || x._1.contains("warn") || x._1.contains("err") ||
//        x._1.contains("crit") || x._1.contains("alert") || x._1.contains("emerg") || x._1.contains("panic"))
//        .keyBy(0)
//        .timeWindow(Time.seconds(10), Time.seconds(5))
//        .sum(1)
//        .map(x => x._1)
//    } else if (3 <= lev && lev < 4) {
//      window = waterMarkStream.map(x => (x._1, 1)).filter(x => x._1.contains("notice") || x._1.contains("warning")
//        || x._1.contains("warn") || x._1.contains("err") || x._1.contains("crit") || x._1.contains("alert")
//        || x._1.contains("emerg") || x._1.contains("panic"))
//        .keyBy(0)
//        .timeWindow(Time.seconds(10), Time.seconds(5))
//        .sum(1)
//        .map(x => x._1)
//    } else if (4 <= lev && lev < 5) {
//      window = waterMarkStream.map(x => (x._1, 1)).filter(x => x._1.contains("warning") || x._1.contains("warn") ||
//        x._1.contains("err") || x._1.contains("crit") || x._1.contains("alert") || x._1.contains("emerg")
//        || x._1.contains("panic"))
//        .keyBy(0)
//        .timeWindow(Time.seconds(10), Time.seconds(5))
//        .sum(1)
//        .map(x => x._1)
//    } else if (5 <= lev && lev < 6) {
//      window = waterMarkStream.map(x => (x._1, 1)).filter(x => x._1.contains("err") || x._1.contains("crit") ||
//        x._1.contains("alert") || x._1.contains("emerg") || x._1.contains("panic"))
//        .keyBy(0)
//        .timeWindow(Time.seconds(10), Time.seconds(5))
//        .sum(1)
//        .map(x => x._1)
//    } else if (6 <= lev && lev < 7) {
//      window = waterMarkStream.map(x => (x._1, 1)).filter(x => x._1.contains("crit") || x._1.contains("alert") ||
//        x._1.contains("emerg") || x._1.contains("panic"))
//        .keyBy(0)
//        .timeWindow(Time.seconds(10), Time.seconds(5))
//        .sum(1)
//        .map(x => x._1)
//    } else if (7 <= lev && lev < 8) {
//      window = waterMarkStream.map(x => (x._1, 1)).filter(x => x._1.contains("alert") || x._1.contains("emerg")
//        || x._1.contains("panic"))
//        .keyBy(0)
//        .timeWindow(Time.seconds(10), Time.seconds(5))
//        .sum(1)
//        .map(x => x._1)
//    } else {
//      window = waterMarkStream.map(x => (x._1, 1)).filter(x => x._1.contains("emerg") || x._1.contains("panic"))
//        .keyBy(0)
//        .timeWindow(Time.seconds(10), Time.seconds(5))
//        .sum(1)
//        .map(x => x._1)
//    }
//
//    val httpHosts = new java.util.ArrayList[HttpHost]
//    val es_host = conf.get("dtc.es.nodes")
//    if (Utils.isEmpty(es_host)) {
//      throw new DtcException("Es_Host is null!")
//    }
//    val host_es = es_host.split(",")
//    for (x <- host_es) {
//      var ip = x.split(":")(0)
//      var host = x.split(":")(1)
//      httpHosts.add(new HttpHost(ip, host.toInt, "http"))
//    }
//
//    val es_index = conf.get("dtc.es.flink.index.name")
//    val es_type = conf.get("dtc.es.flink.type.name")
//    if (Utils.isEmpty(es_index) || Utils.isEmpty(es_type)) {
//      throw new DtcException("Es_index or type is null!")
//    }
//
//    def getEsSink(indexName: String, indexType: String): ElasticsearchSink[String] = {
//      val esSinkFunc: ElasticsearchSinkFunction[String] = new ElasticsearchSinkFunction[String] {
//        def createIndexRequest(element: String): IndexRequest = {
//          return Requests.indexRequest()
//            .index(indexName)
//            .`type`(indexType)
//            .source(element, XContentType.JSON)
//        }
//
//        override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
//          //          val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`(indexType).source(element,XContentType.JSON)
//          //                  indexer.add(indexRequest)
//          indexer.add(createIndexRequest(element))
//        }
//      }
//      val esSinkBuilder = new ElasticsearchSink.Builder[String](httpHosts, esSinkFunc)
//      esSinkBuilder.setBulkFlushMaxActions(10)
//      val esSink: ElasticsearchSink[String] = esSinkBuilder.build()
//      esSink
//    }
//
//    val esSink = getEsSink(es_index, es_type)
//    //            val elasticsearchSink: ElasticsearchSinkFunction[String] = new ElasticsearchSinkFunction[String] {
//    //              override def process(element: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
//    //                val json = new java.util.HashMap[String, String]
//    //                json.put("data", element)
//    //                val indexRequest: IndexRequest = Requests.indexRequest().index().`type`("_doc").source(json)
//    //                indexer.add(indexRequest)
//    //
//    //                return Requests.indexRequest()
//    //                  .index("my-index")
//    //                  .`type`("my-type")
//    //                  .source(json)
//    //              }
//    //            }
//    //            val esSinkBuilder = new ElasticsearchSink.Builder[String](httpHosts, elasticsearchSink)
//    //
//    //    // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
//    //    builder.setBulkFlushMaxActions(1)
//    //
//    //    // provide a RestClientFactory for custom configuration on the internally created REST client
//    //    builder.setRestClientFactory(
//    //      restClientBuilder -> {
//    //        restClientBuilder.setDefaultHeaders(
//    //        ...)
//    //        restClientBuilder.setMaxRetryTimeoutMillis(
//    //        ...)
//    //        restClientBuilder.setPathPrefix(
//    //        ...)
//    //        restClientBuilder.setHttpClientConfigCallback(
//    //        ...)
//    //      }
//    //    )
//    //
//    //    // finally, build and add the sink to the job's pipeline
//    //    input.addSink(esSinkBuilder.build)
//    //
//    //    //FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(brokerList, topic, new SimpleStringSchema());
//    //
//    //    //使用支持仅一次语义的形式
//    //    val myProducer = new FlinkKafkaProducer09[String](topic2, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), props, FlinkKafkaProducer09.Semantic.EXACTLY_ONCE)
//    //
//    window.addSink(esSink)
//    env.execute("StreamingWindowWatermarkScala")
//
//  }
//
//
//  def tranTimeToString(timestamp: String): String = {
//    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val time = fm.format(new Date(timestamp.toLong))
//    time
//  }
//
//
//  //    def getEsSink(indexName: String,indexType:String): ElasticsearchSink[String] = {
//  //      //new接口---> 要实现一个方法
//  //      val esSinkFunc: ElasticsearchSinkFunction[String] = new ElasticsearchSinkFunction[String] {
//  //        override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
//  //          val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`(indexType).source(element)
//  //          indexer.add(indexRequest)
//  //        }
//  //      }
//  //      val esSinkBuilder = new ElasticsearchSink.Builder[String](hostList, esSinkFunc)
//  //      esSinkBuilder.setBulkFlushMaxActions(10)
//  //      val esSink: ElasticsearchSink[String] = esSinkBuilder.build()
//  //      esSink
//  //    }
//
//
//}
//
////class MyMapFunction extends MapFunction[String, Tuple2[String, Long]] {
////  override def map(line: String): Tuple2[String, Long] = {
////    var message = ""
////    if (line.contains("$DTC$")) {
////      val splitDtc: Array[String] = line.split("\\$DTC\\$")
////      val event = splitDtc(0).split("\\$\\$")
////      message += "{" + "\"time\"" + ":" + "\"" + event(0).trim + "\"" + "," + "\"device\"" + ":" + "\"" + event(1).trim +
////        "\"" + "," + "\"" + "level" + "\"" + ":" + "\"" + event(2).trim + "\"" + "," + "\"" + "hostname" + "\"" + ":" +
////        "\"" + event(3).trim + "\"" + "," + "\"" + "message" + "\"" + ":" + "\"" + event(4).trim + "\""
////      var str = ""
////      for (i <- 1 until splitDtc.length) {
////        str = splitDtc(i).trim + "\n"
////      }
////      message += "\"" + "cause" + "\"" + ":" + "\"" + str.trim + "\"" + "}"
////      var time1 = event(0).replace("T", " ").split("\\+")(0).replace("-", "/")
////      var time2 = new Date(time1).getTime
////      return (message, time2)
////    } else {
////      val event = line.split("\\$\\$")
////      message += "{" + "\"time\"" + ":" + "\"" + event(0).trim + "\"" + "," + "\"device\"" + ":" + "\"" + event(1).trim +
////        "\"" + "," + "\"" + "level" + "\"" + ":" + "\"" + event(2).trim + "\"" + "," + "\"" + "hostname" + "\"" + ":" +
////        "\"" + event(3).trim + "\"" + "," + "\"" + "message" + "\"" + ":" + "\"" + event(4).trim + "\"" + "}"
////      var time1 = event(0).replace("T", " ").split("\\+")(0).replace("-", "/")
////      var time2 = new Date(time1).getTime
////      return (message, time2)
////    }
////
////  }
//
//  //  def getEsSink(indexName: String): ElasticsearchSink[String] = {
//  //    //new接口---> 要实现一个方法
//  //    val esSinkFunc: ElasticsearchSinkFunction[String] = new ElasticsearchSinkFunction[String] {
//  //      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
//  //        val json = new java.util.HashMap[String, String]
//  //        json.put("data", element)
//  //        val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(json)
//  //        indexer.add(indexRequest)
//  //      }
//  //    }
//  //    val esSinkBuilder = new ElasticsearchSink.Builder[String](hostList, esSinkFunc)
//  //    esSinkBuilder.setBulkFlushMaxActions(10)
//  //    val esSink: ElasticsearchSink[String] = esSinkBuilder.build()
//  //    esSink
//  //  }
//
//
//  //  def getEsSink(indexName: String): ElasticsearchSink[String] = {
//  //    //new接口---> 要实现一个方法
//  //    val esSinkFunc: ElasticsearchSinkFunction[String] = new ElasticsearchSinkFunction[String] {
//  //      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
//  //        val json = new java.util.HashMap[String, String]
//  //        json.put("data", element)
//  //        val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(json)
//  //        indexer.add(indexRequest)
//  //      }
//  //    }
//  //    val esSinkBuilder = new ElasticsearchSink.Builder[String](hostList, esSinkFunc)
//  //    esSinkBuilder.setBulkFlushMaxActions(10)
//  //    val esSink: ElasticsearchSink[String] = esSinkBuilder.build()
//  //    esSink
//  //  }
//
//
////}
