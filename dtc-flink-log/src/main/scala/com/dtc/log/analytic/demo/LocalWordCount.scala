package com.dtc.log.analytic.demo

import org.apache.flink.streaming.api.scala._

/**
  * Created on 2020-06-20
  *
  * @author :hao.li
  */
object LocalWordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile("/Users/lixuewei/workspace/private/play_demo_work/flink/src/test.txt")
    //    val text = env.readTextFile("/Users/lixuewei/workspace/private/play_demo_work/flink/src/test.txt")
    val counts: DataStream[(String, Int)] = text.flatMap(_.toLowerCase.split("\\W+"))
      //      text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map {
        (_, 1)
      }
      .keyBy(0)
      .sum(1)

    counts.print()
    env.execute("Scala WordCount Example")

    val e = Seq("I love", "coding scala")
    getWords(e).foreach(println(_))
  }

  def getWords(lines: Seq[String]): Seq[String] = {
    lines flatMap (_ split " ")
  }

}
