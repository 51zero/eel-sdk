package io.eels.plan

import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Try}

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
  val failureEnabled = Try(config.getBoolean("eel.execution.plan.fail.enable")).toOption.fold(false)(b => b)

  def raiseExceptionOnFailure[T](futures: Seq[Future[T]]): Unit = {
    if (failureEnabled) {
      futures.map(f => f.value.get).collectFirst { case Failure(t) => t } match {
        case Some(t) => throw t
        case _ =>
      }
    }
  }
}