package io.eels.component.kafka

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.sksamuel.kafka.embedded.{EmbeddedKafkaConfig, EmbeddedKafka}
import io.eels.{SchemaType, Column, Row, Field}
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class KafkaSourceTest extends WordSpec with Matchers with BeforeAndAfterAll {

  val config = EmbeddedKafkaConfig()
  val kafka = new EmbeddedKafka(config)
  kafka.start()

  "KafkaSource" should {
    "read from topic" in {

      val topic = "kafka-source-test"

      val producerProps = new Properties
      producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + config.kafkaPort)
      val producer = new KafkaProducer[String, String](producerProps, new StringSerializer, new StringSerializer)

      for ( _ <- 1 to 100 ) {
        val record = new ProducerRecord[String, String](topic, """{ "name" : "sam", "location" : "london" }""")
        producer.send(record).get(1, TimeUnit.MINUTES)
      }

      producer.close(1, TimeUnit.MINUTES)

      val sourceConfig = KafkaSourceConfig("localhost:" + config.kafkaPort, topic)
      val source = KafkaSource(sourceConfig, Set(topic), JsonKafkaDeserializer)
      val rows = source.toList.run
      rows.size shouldBe 100
      rows.head shouldBe Row(
        List(Column("name", SchemaType.String, false), Column("location", SchemaType.String, false)),
        List(Field("sam"), Field("london"))
      )
    }
  }

  override protected def afterAll(): Unit = {
    kafka.stop()
  }
}
