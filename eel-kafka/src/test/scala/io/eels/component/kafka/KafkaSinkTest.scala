package io.eels.component.kafka

import java.util
import java.util.{Properties, UUID}

import io.eels.schema.{Field, StringType, StructType}
import io.eels.{Frame, Row}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConverters._

class KafkaSinkTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  implicit val kafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 6001,
    zooKeeperPort = 6000
  )
  EmbeddedKafka.start()

  val schema = StructType(
    Field("name", StringType, nullable = true),
    Field("location", StringType, nullable = true)
  )

  val frame = Frame.fromValues(
    schema,
    Vector("clint eastwood", UUID.randomUUID().toString),
    Vector("elton john", UUID.randomUUID().toString)
  )

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
  }

  "KafkaSink" should "support default implicits" in {

    val topic = "mytopic"

    val properties = new Properties()
    properties.put("bootstrap.servers", s"localhost:${kafkaConfig.kafkaPort}")
    properties.put("group.id", "test")
    properties.put("auto.offset.reset", "earliest")

    val producer = new KafkaProducer[String, Row](properties, StringSerializer, RowSerializer)
    val sink = KafkaSink(topic, producer)

    val consumer = new KafkaConsumer[String, String](properties, StringDeserializer, StringDeserializer)
    consumer.subscribe(util.Arrays.asList(topic))

    frame.to(sink)
    producer.close()

    val records = consumer.poll(4000)
    records.iterator().asScala.map(_.value).toList shouldBe frame.collect.map {
      case Row(_, values) => values.mkString(",")
    }.toList
  }
}

object RowSerializer extends Serializer[Row] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
  override def serialize(topic: String, data: Row): Array[Byte] = data.values.mkString(",").getBytes
  override def close(): Unit = ()
}

object StringSerializer extends Serializer[String] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
  override def close(): Unit = ()
  override def serialize(topic: String, data: String): Array[Byte] = data.getBytes
}

object StringDeserializer extends Deserializer[String] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
  override def close(): Unit = ()
  override def deserialize(topic: String, data: Array[Byte]): String = new String(data)
}