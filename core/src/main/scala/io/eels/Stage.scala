package io.eels

trait Supplier {
  def request(k: Int, receiver: Receiver)
}

trait Receiver {
  def receive(k: Int)
}
