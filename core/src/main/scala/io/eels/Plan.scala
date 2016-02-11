package io.eels

trait Plan[T] {
  def run: T
}

trait ConcurrentPlan[T] extends Plan[T] {
  final def run: T = runConcurrent(1)
  def runConcurrent(workers: Int): T
}