package io.eels.plan

import java.util.concurrent.TimeUnit

import com.sksamuel.scalax.concurrent.Futures
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

trait Plan {

  val config = ConfigFactory.load()

  val tasks = {
    val tasks = config.getInt("eel.execution.tasks")
    // there's no point creating more tasks than available cores, or we'll just preempt our own tasks.
    if (tasks == -1) Runtime.getRuntime.availableProcessors / 2
    else tasks
  }

  val timeout = config.getDuration("eel.execution.timeout", TimeUnit.NANOSECONDS).nanos

  // do we want to fail execution if one of the plan tasks fail
  val failureEnabled = Try(config.getBoolean("eel.execution.plan.fail.enable")).toOption.getOrElse(false)

  def raiseExceptionOnFailure[T](futures: Seq[Future[T]])(implicit executor: ExecutionContext): Unit = {
    if (failureEnabled) {
      Await.result(Futures.firstThrowableOf(futures), 1.minute) match {
        case Some(t) => throw t
        case _ =>
      }
    }
  }
}