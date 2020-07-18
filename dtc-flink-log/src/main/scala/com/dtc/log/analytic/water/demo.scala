package com.dtc.log.analytic.water
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
/**
  * Created on 2020-06-20
  *
  * @author :hao.li
  */
object demo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val input=env.fromCollection(List(("a", 1L, 1), ("b", 1L, 1), ("b", 3L, 1)))
//    val input = env.fromCollection(List(("a", 1L, 1), ("b", 1L, 1), ("b", 3L, 1)))
//    val withtimestampsAndWatermarks = input.assignAscendingTimestamps(t => t._3)
//    val result = withtimestampsAndWatermarks.keyBy(0).timeWindow(Time.seconds(10)).sum("_2")
//    result.print()
    input.print()
    env.execute()
  }
}
