package io.eels.actions

import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{CountDownLatch, TimeUnit}

import io.eels.datastream.DataStream

import scala.concurrent.ExecutionContext

case class CountAction(ds: DataStream) extends Action {

  private val executor = ExecutionContext.Implicits.global

  def execute: Long = {
    val adder = new LongAdder
    val partitions = ds.partitions
    val latch = new CountDownLatch(partitions.size)
    partitions.foreach { partition =>
      executor.execute(new Runnable {
        override def run(): Unit = {
          partition.iterator.foreach(_ => adder.add(1))
          latch.countDown()
        }
      })
    }
    latch.await(1, TimeUnit.DAYS)
    adder.sum()
  }
}