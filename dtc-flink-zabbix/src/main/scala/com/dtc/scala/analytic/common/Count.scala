package com.dtc.scala.analytic.common

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

/**
  * Created on 2019-06-12
  *
  * @author :hao.li
  */
class Count {
  var counterMap= new mutable.HashMap[String,AtomicLong]
  var atomicLong = new AtomicLong(0)
  val COUNTER_EVENTS_RECEIVED: String = "counter.events.received"
  val COUNTER_EVENTS_ERROR: String = "counter.events.error"
  val COUNTER_EVENTS_RIGHT: String = "counter.events.right"

  def increment(counter:String): Long ={
    if(counterMap.get(counter).isEmpty){
      counterMap.put(counter,atomicLong)
      return 0L
    }else{
      return counterMap(counter).incrementAndGet()
    }

  }
  def getCount(counter:String):Long={
    return counterMap(counter).toString.toLong
  }

}
