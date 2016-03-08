package io.eels.component.jms

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import javax.jms.{Message, MessageConsumer, MessageListener, TextMessage}

import com.sksamuel.scalax.collection.BlockingQueueConcurrentIterator
import io.eels._

case class JmsSource(consumer: MessageConsumer, limit: Int = 1) extends Source {

  override def parts: Seq[Part] = {

    val count = new AtomicLong(0)

    val part = new Part {
      override def reader = new SourceReader {
        val buffer = new LinkedBlockingQueue[Any](100)
        consumer.setMessageListener(new MessageListener {
          override def onMessage(message: Message): Unit = {
            buffer.add(message.asInstanceOf[TextMessage].getText)
            message.acknowledge()
            if (count.incrementAndGet == limit) {
              close()
            }
          }
        })
        override def close(): Unit = {
          logger.debug("Closing JMS message consumer")
          buffer.add(InternalRow.PoisonPill)
          consumer.close()
        }

        override def iterator: Iterator[InternalRow] = {
          BlockingQueueConcurrentIterator(buffer, InternalRow.PoisonPill).map { any => Seq(any) }
        }
      }
    }
    Seq(part)
  }

  override def schema: Schema = Schema(Seq.empty[String])
}
