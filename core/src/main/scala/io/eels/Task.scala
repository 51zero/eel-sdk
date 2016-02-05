package io.eels

import scala.concurrent.ExecutionContext

/**
  * T is the result type when the Task is executed.
  */
trait Task[T] {
  def run(implicit executor: ExecutionContext): T
}