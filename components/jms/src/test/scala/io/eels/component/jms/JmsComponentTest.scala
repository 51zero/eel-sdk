package io.eels.component.jms

import javax.jms.{DeliveryMode, Session}

import io.eels.{Schema, Row}
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.BrokerService
import org.scalatest.{Matchers, WordSpec}

class JmsComponentTest extends WordSpec with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  val broker = new BrokerService()
  broker.addConnector("tcp://localhost:61616")
  broker.setPersistent(false)
  broker.start()

  "JmsComponent" should {
    "receive messages" in {

      val connectionFactory = new ActiveMQConnectionFactory("vm://localhost")

      val connection = connectionFactory.createConnection()
      connection.start()

      val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
      val destination = session.createQueue("test")

      val producer = session.createProducer(destination)
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT)

      val text = "Hello world"
      val message = session.createTextMessage(text)
      producer.send(message)

      val consumer = session.createConsumer(destination)
      val rows = JmsSource(consumer).toSet
      rows shouldBe Set(Row(Schema(List.empty[String]), List("Hello world")))

      session.close()
      connection.close()
    }
  }
}
