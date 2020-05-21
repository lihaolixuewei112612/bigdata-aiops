package scala

import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created on 2019-05-27
  *
  * @author :hao.li
  */
object Main {
  def log: Logger = LoggerFactory.getLogger(Main.getClass)
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.fromElements(1, 2, 3, 4, 5).map { t: Int => t }
    val iterated = dataStream.iterate((input: ConnectedStreams[Int, String]) => {
      val head = input.map(i => (i + 1).toString, s => s)
      (head.filter(_ == "2"), head.filter(_ != "2"))
    }, 100)
    iterated.print
    log.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    env.execute("Window Stream WordCount")
  }
}
