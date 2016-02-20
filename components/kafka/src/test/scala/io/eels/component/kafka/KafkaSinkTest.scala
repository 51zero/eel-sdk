package io.eels.component.kafka

import java.util
import java.util.Properties

import com.sksamuel.kafka.embedded.{EmbeddedKafka, EmbeddedKafkaConfig}
import io.eels.Frame
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import scala.collection.JavaConverters._

class KafkaSinkTest extends WordSpec with Matchers with BeforeAndAfterAll {

  import scala.concurrent.ExecutionContext.Implicits.global

  val config = EmbeddedKafkaConfig()
  val kafka = new EmbeddedKafka(config)
  kafka.start()

  "KafkaSink" should {
    "write to topic" in {

      val topic = "kafka-sink-test"

      val frame = Frame(
        Map("name" -> "dover castle", "location" -> "kent"),
        Map("name" -> "tower of london", "location" -> "london"),
        Map("name" -> "hever castle", "location" -> "kent")
      )

      val kafkaSinkConfig = KafkaSinkConfig("localhost:" + config.kafkaPort)
      frame.to(KafkaSink(kafkaSinkConfig, topic, JsonKafkaSerializer))

      val consumerProps = new Properties
      consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + config.kafkaPort)
      consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkasinktest")
      consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      val consumer = new KafkaConsumer[String, String](consumerProps, new StringDeserializer, new StringDeserializer)
      consumer.subscribe(util.Arrays.asList(topic))

      val records = consumer.poll(5000)
      records.asScala.toList.size shouldBe 3
    }
  }

  override protected def afterAll(): Unit = {
    kafka.stop()
  }
}
