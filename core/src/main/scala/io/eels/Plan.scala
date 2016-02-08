package io.eels

trait Plan[T] {
  def run: T
}

trait ConcurrentPlan[T] extends Plan[T] {
  def run: T = runConcurrent(1)
  def runConcurrent(concurrency: Int): T
}