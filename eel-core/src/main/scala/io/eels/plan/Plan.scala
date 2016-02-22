package io.eels.plan

import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

trait Plan {
  val config = ConfigFactory.load()
  val slices = config.getInt("eel.execution.tasks")
  val timeout = config.getDuration("eel.execution.timeout", TimeUnit.NANOSECONDS).nanos
}