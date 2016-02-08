package io.eels

import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class ToListPlan(frame: Frame) extends ConcurrentPlan[List[Row]] {

  override def run(concurrency: Int): List[Row] = {

    import com.sksamuel.scalax.concurrent.ExecutorImplicits._

    val executor = Executors.newFixedThreadPool(concurrency)
    implicit val ec = ExecutionContext.fromExecutorService(executor)

    val fs = frame.parts.map { part =>
      executor.submit {
        part.iterator.toList
      }
    }.toList

    val f = Future.sequence(fs)
    val list = Await.result(f, 1.days).flatten

    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.DAYS)

    list
  }
}
