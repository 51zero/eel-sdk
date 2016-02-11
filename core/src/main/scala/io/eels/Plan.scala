package io.eels

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

trait Plan[T] {
  def run: T
}

trait ConcurrentPlan[T] extends Plan[T] {
  val singleThreadExecutor = Executors.newSingleThreadExecutor
  val singleThreadExecutionContext = ExecutionContext.fromExecutor(singleThreadExecutor)
  final def run: T = runConcurrent(1)(singleThreadExecutionContext)
  def runConcurrent(workers: Int)(implicit executor: ExecutionContext): T
}