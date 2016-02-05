package io.eels

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask

class SourceActor(reader: Reader) extends Actor {

  val iter = reader.iterator

  override def receive: Actor.Receive = {
    case Request(k) =>
      val ks = iter.take(k).toList
      sender ! Supply(ks)
  }
}

class ProxyActor extends Actor {
  override def receive: Actor.Receive = {
    case Request(k) =>
  }
}

class Frame2 {
  def map(f: Row => Row): Frame2 = new Frame2 {

  }
}

class FrameMapActor(frame: ActorRef, f: Row => Row) extends Actor {
  override def receive: Actor.Receive = {
    case Request(k) =>
      val reply = sender()
      frame ? Request(k) onSuccess {
        case Supply(ks) => reply ! Supply(ks.map(f))
      }
  }
}

case class Request(k: Int)

case class Supply(ks: Seq[Row])
