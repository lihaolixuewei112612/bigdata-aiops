package com.dtc.scala.analytic

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConverters._

/**
  * Created on 2019-09-06
  *
  * @author :hao.li
  */
object test {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val initial = env.fromElements(0)

    val count = initial.iterate(100000) { iterationInput: DataSet[Int] =>
      val result = iterationInput.map { i =>
        val x = Math.random()
        val y = Math.random()
        i + (if (x * x + y * y < 1) 1 else 0)
      }
      result
    }
    val result = count map { c => c / 100000.0 * 4 }
    result.print()
    env.execute("Iterative Pi Example")
  }
}
