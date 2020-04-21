package com.dtc.scala.analytic.lihao

import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.slf4j.LoggerFactory

import scala.util.Random

/**
  * Created on 2019-08-12
  *
  * @author :hao.li
  */
class SimulatedEventSource extends RichParallelSourceFunction[String] {

  //  val LOG = LoggerFactory.getLogger(classOf[SimulatedEventSource])
  @volatile private var running = true
  val channelSet = Seq("a", "b", "c", "d")
  val behaviorTypes = Seq(
    "INSTALL", "OPEN", "BROWSE", "CLICK",
    "PURCHASE", "CLOSE", "UNINSTALL")
  val rand = Random

  override def run(ctx: SourceContext[(String)]): Unit = {
    val numElements = Long.MaxValue
    var count = 0L

    while (running && count < numElements) {
      val channel = channelSet(rand.nextInt(channelSet.size))
      val str = "{\"labels\":{\"__name__\":\"linux_prometheus_tsdb_compaction_chunk_size_bytes_bucket\",\"instance\":\"localhost:9093\",\"job\":\"prometheus\",\"le\":\"364.5\"},\"name\":\"node_c\",\"timestamp\":\"2019-08-14T03:55:36Z\",\"value\":\"9\"}"
      //      val str="{\"code\":\"101|101|101_101|105|106\",\"host\":\"a\",\"time\":\"2019-07-22T03:55:36Z\",\"value\":\"10\"}"
      //      val str1="{\"code\":\"101|101|101_101|105|107\",\"host\":\"a\",\"time\":\"2019-07-22T03:55:36Z\",\"value\":\"1000\"}"
      val event = generateEvent()
      ctx.collect(str)
      //      ctx.collect(event)
      //            LOG.info("Event: " + event)
      //            val ts = event(0).toLong
      //            ctx.collectWithTimestamp((channel, event.mkString("\t")), ts)
      count += 1
      TimeUnit.MILLISECONDS.sleep(5000L)
    }
  }

  private def generateEvent(): String = {
    val channel = channelSet(rand.nextInt(channelSet.size))
    val dt = readableDate
    val value = rand.nextInt(9)
    val next = rand.nextInt(2) + 1
    val result = rand.nextInt(30)
    val test1 = rand.nextInt(3) + 5
    val test2 = rand.nextInt(4) + 6

    val id = UUID.randomUUID().toString
    val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
    // (ts, readableDT, id, behaviorType)
    val message = "{" + "\"code\"" + ":" + "\"" + 101 + "|" + 101 + "|" + 101 + "_" + 10 + next.toString + "|" + 10 + test1.toString + "|" + 10 + test2.toString + "\"" + "," + "\"host\"" + ":" + "\"" + channel.toString +
      "\"" + "," + "\"" + "time" + "\"" + ":" + "\"" + dt._1.toString + "\"" + "," + "\"" + "value" + "\"" + ":" +
      "\"" + result + "\"" + "}"
    message
  }

  private def readableDate = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val ts = System.nanoTime
    val dt = new Date(ts)
    (ts, df.format(dt))
  }

  override def cancel(): Unit = running = false
}
