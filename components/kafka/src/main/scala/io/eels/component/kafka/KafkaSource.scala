package io.eels.component.kafka

import java.util.{Properties, UUID}

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Column, Field, FrameSchema, Reader, Row, Source}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.collection.JavaConverters._

case class KafkaSourceConfig(brokerList: String,
                             consumerGroup: String,
                             autoOffsetReset: String = "earliest",
                             enableAutoCommit: Boolean = false)

case class KafkaSource(config: KafkaSourceConfig, topics: Set[String], deserializer: KafkaDeserializer)
  extends Source with StrictLogging {

  override def schema: FrameSchema = {

    val consumerProps = new Properties
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokerList)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "schema-consumer_" + UUID.randomUUID)
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](
      consumerProps,
      new ByteArrayDeserializer,
      new ByteArrayDeserializer
    )
    consumer.subscribe(topics.toList.asJava)

    val record = consumer.poll(4000).asScala.take(1).toList.head
    consumer.close()
    val row = deserializer(record.value)

    FrameSchema(row.columns)
  }

  override def readers: Seq[Reader] = {

    val reader = new Reader {

      val consumerProps = new Properties
      consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokerList)
      consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, config.consumerGroup)
      consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.enableAutoCommit.toString)
      consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.autoOffsetReset)
      val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](
        consumerProps,
        new ByteArrayDeserializer,
        new ByteArrayDeserializer
      )
      consumer.subscribe(topics.toList.asJava)

      val records = consumer.poll(4000).asScala.toList
      logger.debug(s"Read ${records.size} records from kafka")
      consumer.close()
      logger.debug("Closed kafka consumer")

      override def close(): Unit = ()
      override def iterator: Iterator[Row] = records.iterator.map { record =>
        val bytes = record.value()
        deserializer(bytes)
      }
    }
    Seq(reader)
  }
}

trait KafkaDeserializer {
  def apply(bytes: Array[Byte]): Row
}

object JsonKafkaDeserializer extends KafkaDeserializer {
  val mapper = new ObjectMapper
  override def apply(bytes: Array[Byte]): Row = {
    val node = mapper.readTree(bytes)
    val columns = node.fieldNames.asScala.map(Column.apply).toList
    val fields = node.fieldNames.asScala.map { name => Field(node.get(name).textValue) }.toList
    Row(columns, fields)
  }
}