package com.dtc.log.analytic.demo
import org.apache.flink.streaming.api.scala._
/**
  * Created on 2020-06-20
  *
  * @author :hao.li
  */
object Main {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    val dataStream = env.fromElements(("a", 3), ("d", 4), ("c", 2), ("c", 5), ("a", 5))
    //    val flatresult = dataStream.flatMap(t=>t._1+t._2)
    //    flatresult.print
    val text = env.fromElements("To be, or not to be,--that is the question:-- lihao",
      "Whether 'tis nobler in the mind to suffer",
      "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of lihao troubles,")

    val data = text.flatMap(str=>str.split(" ")).filter(_.length>2).map(t=>(t,1)).keyBy(0).sum(1)
    data.print
    env.execute("Window Stream WordCount")
  }
}

