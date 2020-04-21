package com.dtc.scala.analytic.common

/**
  * Created on 2019-06-03
  *
  * @author :hao.li
  */
object LevelEnum {

  object Level extends Enumeration {
    type Level = Value
    val INFO, DEBUG, NOTICE, WARNING, ERR, CRIT, ALTER, EMERG = Value
  }


  def getIndex(name: String): Int = {
    for (c <- Level.values) {
      if (c.toString.toLowerCase == name) {
        return c.id + 1
      }
    }
    1
  }

}
