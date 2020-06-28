package com.dtc.log.analytic.demo

/**
  * Created on 2020-06-20
  *
  * @author :hao.li
  */
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
object WordCountJob {
  def main(args: Array[String]) {
    // 1.设置运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2.创造测试数据
    val text = env.readTextFile("hdfs://10.3.6.7:9000/user/hadoop/demo/abc.txt")
    val counts =text.flatMap(_.toLowerCase.split("\\W+")).map{(_,1)}.groupBy(0).sum(1)
    //4.打印测试结构
    counts.print()
    env.execute("Scala WordCount Example")
  }

}
