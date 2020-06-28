package com.dtc.log.analytic.common

/**
  * Created on 2020-06-20
  *
  * @author :hao.li
  */
object Utils {

  def isEmpty(str: String): Boolean = {
    if (str.length == 0 || str == null) {
      return true
    } else {
      return false
    }
  }

}
