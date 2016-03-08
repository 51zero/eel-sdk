package io.eels.plan

import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

trait Plan {
  val config = ConfigFactory.load()
  val tasks = {
    val tasks = config.getInt("eel.execution.tasks")
    // there's no point creating more tasks than available cores, or we'll just preempt our own tasks.
    if (tasks == -1) Runtime.getRuntime.availableProcessors / 2
    else tasks
  }
  val timeout = config.getDuration("eel.execution.timeout", TimeUnit.NANOSECONDS).nanos
}