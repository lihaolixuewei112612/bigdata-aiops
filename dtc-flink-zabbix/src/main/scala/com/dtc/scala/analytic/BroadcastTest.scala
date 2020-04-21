package com.dtc.scala.analytic


import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConverters._

/**
  * Created on 2019-09-05
  *
  * @author :hao.li
  */
object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements("1", "2", "3", "4", "5")
    val ds2 = env.fromElements("a", "b", "c", "d", "e")

    ds1.map(new RichMapFunction[String, (String, String)] {
      private var ds3: Traversable[String] = null

      override def open(parameters: Configuration) {
        ds3 = getRuntimeContext.getBroadcastVariable[String]("broadCast").asScala
      }

      def map(t: String): (String, String) = {
        var result = ""
        for (broadVariable <- ds3) {
          result = result + broadVariable + " "
        }
        (t, result)
      }
    }).withBroadcastSet(ds2, "broadCast").print()
  }
}
