//package com.dtc.analytic.scala.works
//
//import java.text.SimpleDateFormat
//import java.util.{Date, Properties}
//
//import com.dtc.analytic.scala.common.{DtcConf, Utils}
//import com.dtc.analytic.scala.dtcexpection.DtcException
//import org.apache.flink.api.common.functions.RuntimeContext
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
//import org.mortbay.util.ajax.JSON
//
///**
//  * Created on 2019-05-27
//  *
//  * @author :hao.li
//  */
//object StreamingWindowWatermarkScala {
//  def main(args: Array[String]): Unit = {
//    try {
//      DtcConf.setup()
//    }
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)
//    val conf = DtcConf.getConf()
//
//    val topic = "t1"
//    val prop = new Properties()
//    prop.setProperty("bootstrap.servers", "192.168.200.10:9092")
//    prop.setProperty("group.id", "con1")
//    val myConsumer = new FlinkKafkaConsumer09[String](topic, new SimpleStringSchema(), prop)
//    val text = env.addSource(myConsumer)
//
//    val inputMap = text.map(line => {
//      val arr = line.split(",")
//      (arr(0), arr(1).toLong)
//    })
//
//    val waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
//      var currentMaxTimestamp = 0L
//      var maxOutOfOrderness = 10000L // 最大允许的乱序时间是10s
//
//      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//
//      override def getCurrentWatermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
//
//      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long) = {
//        val timestamp = element._2
//        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
//        val id = Thread.currentThread().getId
//        println("currentThreadId:" + id + ",key:" + element._1 + ",eventtime:[" + element._2 + "|" + sdf.format(element._2) + "],currentMaxTimestamp:[" + currentMaxTimestamp + "|" + sdf.format(currentMaxTimestamp) + "],watermark:[" + getCurrentWatermark().getTimestamp + "|" + sdf.format(getCurrentWatermark().getTimestamp) + "]")
//        timestamp
//      }
//    })
//
//    val window = waterMarkStream.map(x => (x._2, 1)).timeWindowAll(Time.seconds(1), Time.seconds(1)).sum(1).map(x => "time:" + tranTimeToString(x._1.toString) + "  count:" + x._2)
//    // .window(TumblingEventTimeWindows.of(Time.seconds(3))) //按照消息的EventTime分配窗口，和调用TimeWindow效果一样
//
//    //.max(0).map(x=>x._1)
//    val httpHosts = new java.util.ArrayList[HttpHost]
//    val es_host = conf.get("dtc.es.nodes")
//    if (Utils.isEmpty(es_host)) {
//      throw new DtcException("Es_Host is find!")
//    }
//    val host_es = es_host.split(",")
//    for(x<-host_es){
//      var ip = x.split(":")(0)
//      var host =x.split(":")(1)
//      httpHosts.add(new HttpHost(ip, host.toInt, "http"))
//    }
//
//    val elasticsearchSink: ElasticsearchSinkFunction[String] = new ElasticsearchSinkFunction[String] {
//      override def process(element: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
//        val json = new java.util.HashMap[String, String]
//        json.put("data", element)
//        val indexRequest: IndexRequest = Requests.indexRequest().index().`type`("_doc").source(json)
//        indexer.add(indexRequest)
//
//        return Requests.indexRequest()
//          .index("my-index")
//          .`type`("my-type")
//          .source(json)
//      }
//    }
//    val esSinkBuilder = new ElasticsearchSink.Builder[String](httpHosts, elasticsearchSink)
//
//    // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
//    builder.setBulkFlushMaxActions(1)
//
//    // provide a RestClientFactory for custom configuration on the internally created REST client
//    builder.setRestClientFactory(
//      restClientBuilder -> {
//        restClientBuilder.setDefaultHeaders(
//        ...)
//        restClientBuilder.setMaxRetryTimeoutMillis(
//        ...)
//        restClientBuilder.setPathPrefix(
//        ...)
//        restClientBuilder.setHttpClientConfigCallback(
//        ...)
//      }
//    )
//
//    // finally, build and add the sink to the job's pipeline
//    input.addSink(esSinkBuilder.build)
//
//    //FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(brokerList, topic, new SimpleStringSchema());
//
//    //使用支持仅一次语义的形式
//    val myProducer = new FlinkKafkaProducer09[String](topic2, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), props, FlinkKafkaProducer09.Semantic.EXACTLY_ONCE)
//
//    window.addSink(myProducer)
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
//  def getEsSink(indexName: String): ElasticsearchSink[String] = {
//    //new接口---> 要实现一个方法
//    val esSinkFunc: ElasticsearchSinkFunction[String] = new ElasticsearchSinkFunction[String] {
//      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
//        val json = new java.util.HashMap[String, String]
//        json.put("data", element)
//        val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(json)
//        indexer.add(indexRequest)
//      }
//    }
//    val esSinkBuilder = new ElasticsearchSink.Builder[String](hostList, esSinkFunc)
//    esSinkBuilder.setBulkFlushMaxActions(10)
//    val esSink: ElasticsearchSink[String] = esSinkBuilder.build()
//    esSink
//  }
//
//
//}
