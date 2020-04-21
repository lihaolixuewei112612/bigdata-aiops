package com.dtc.scala.analytic.common

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
/**
  * Created on 2019-06-12
  *
  * @author :hao.li
  */
object CountUtils {
  var counterMap = new mutable.HashMap[String, AtomicLong]()
  val COUNTER_EVENTS_RECEIVED: String = "counter.events.received"
  val COUNTER_EVENTS_ERROR: String = "counter.events.error"
  val COUNTER_EVENTS_RIGHT: String = "counter.events.right"
  val count = new Count

  def incrementEventReceivedCount: Long = {
    return count.increment(COUNTER_EVENTS_RECEIVED)
  }

  def incrementEventRightCount: Long = {
    return count.increment(COUNTER_EVENTS_RIGHT)
  }

  def incrementEventErrorCount: Long = {
    return count.increment(COUNTER_EVENTS_ERROR)
  }

  def getEventReceivedCount: Long = {
    return count.getCount(COUNTER_EVENTS_RECEIVED)
  }

  def getEventRightCount: Long = {
    return count.getCount(COUNTER_EVENTS_RIGHT)
  }

  def getEventErrorCount: Long = {
    return count.getCount(COUNTER_EVENTS_ERROR)
  }
}

