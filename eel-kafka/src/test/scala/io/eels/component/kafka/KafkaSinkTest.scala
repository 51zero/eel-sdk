package io.eels.component.kafka

import java.util
import java.util.{Properties, UUID}

import io.eels.Row
import io.eels.datastream.DataStream
import io.eels.schema.{Field, StringType, StructType}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.util.Try

class KafkaSinkTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  implicit val kafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 6001,
    zooKeeperPort = 6000
  )
  Try {
    EmbeddedKafka.start()
  }

  val schema = StructType(
    Field("name", StringType, nullable = true),
    Field("location", StringType, nullable = true)
  )

  val ds = DataStream.fromValues(
    schema,
    Seq(
      Vector("clint eastwood", UUID.randomUUID().toString),
      Vector("elton john", UUID.randomUUID().toString)
    )
  )

  "KafkaSink" should "support default implicits" ignore {

    val topic = "mytopic-" + System.currentTimeMillis()

    val properties = new Properties()
    properties.put("bootstrap.servers", s"localhost:${kafkaConfig.kafkaPort}")
    properties.put("group.id", "test")
    properties.put("auto.offset.reset", "earliest")

    val producer = new KafkaProducer[String, Row](properties, StringSerializer, RowSerializer)
    val sink = KafkaSink(topic, producer)

    val consumer = new KafkaConsumer[String, String](properties, StringDeserializer, StringDeserializer)
    consumer.subscribe(util.Arrays.asList(topic))

    ds.to(sink)
    producer.close()

    val records = consumer.poll(4000)
    records.iterator().asScala.map(_.value).toList shouldBe ds.collect.map {
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