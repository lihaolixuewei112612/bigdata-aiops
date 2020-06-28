package com.dtc.log.analytic.demo

/**
  * Created on 2020-06-20
  *
  * @author :hao.li
  */
object EnumTest {

  /** *
    * 定义一个星期的枚举
    */
  object WeekDay extends Enumeration {
    type WeekDay = Value //声明枚举对外暴露的变量类型
    val Mon = Value("mon")
    val Tue = Value("tue")
    val Wed = Value("wed")
    val Thu = Value("thu")
    val Fri = Value("fri")
    val Sat = Value("sta")
    val Sun = Value("sun")

    //    def checkExists(day:String) = this.values.exists(_.toString==day) //检测是否存在此枚举值
    //    def isWorkingDay(day:WeekDay) = ! ( day==Sat || day == Sun) //判断是否是工作日
    //    def showAll = this.values.foreach(println) // 打印所有的枚举值
  }


  def getIndex(name: String): Int = {
    for (c <- WeekDay.values) {
      if (c.toString == name) {
        return c.id + 1
      }
    }
    1
  }


  def main(args: Array[String]): Unit = {

    print(getIndex("sun"))

  }

}
