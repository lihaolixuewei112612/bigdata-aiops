package com.dtc.log.analytic.demo

/**
  * Created on 2020-06-20
  *
  * @author :hao.li
  */
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object WordCount {
  @throws[Exception]
  def main(args: Array[String]): Unit = { //定义socket的端口号
    var port = 0
    try {
      val parameterTool = ParameterTool.fromArgs(args)
      port = parameterTool.getInt("port")
    } catch {
      case e: Exception =>
        System.err.println("没有指定port参数，使用默认值9000")
        port = 9000
    }
    //获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //连接socket获取输入的数据
    val text = env.readTextFile("hdfs://10.3.6.7:9000/user/hadoop/demo/abc.txt")
    //计算数据
    val windowCount = text.flatMap(new FlatMapFunction[String, WordCount.WordWithCount]() {
      override def flatMap(value: String, out: Collector[WordCount.WordWithCount]): Unit = {
        val splits = value.split("\\s")
        for (word <- splits) {
          out.collect(new WordCount.WordWithCount(word, 1L))
        }
      }
    }).keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1))
      .sum("count")

      windowCount.setParallelism(1) //使用一个并行度

    //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
    env.execute("streaming word count")
  }

  /**
    * 主要为了存储单词以及单词出现的次数
    */
  class WordWithCount() {
    var word: String = null
    var count = 0L

    def this(word: String, count: Long) {
      this()
      this.word = word
      this.count = count
    }

    override def toString: String = "WordWithCount{" + "word='" + word + '\'' + ", count=" + count + '}'
  }

}
