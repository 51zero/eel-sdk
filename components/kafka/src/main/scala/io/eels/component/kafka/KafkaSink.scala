package io.eels.component.kafka

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{InternalRow, FrameSchema, Sink, Writer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.concurrent.duration._

case class KafkaSinkConfig(brokerList: String,
                           autoOffsetReset: String = "earliest",
                           enableAutoCommit: Boolean = false,
                           shutdownTimeout: FiniteDuration = 1.hour)

case class KafkaSink(config: KafkaSinkConfig, topic: String, serializer: KafkaSerializer)
  extends Sink
    with StrictLogging {

  override def writer: Writer = new Writer {

    val producerProps = new Properties
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokerList)
    val producer = new KafkaProducer(producerProps, new ByteArraySerializer, new ByteArraySerializer)
    logger.info(s"Created kafka producer [${config.brokerList}]")

    override def close(): Unit = {
      logger.info("Shutting down kafka producer")
      producer.close(config.shutdownTimeout.toNanos, TimeUnit.NANOSECONDS)
    }

    override def write(row: InternalRow, schema: FrameSchema): Unit = {
      val bytes = serializer(row, schema)
      val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, bytes)
      producer.send(record)
    }
  }
}

trait KafkaSerializer {
  def apply(row: InternalRow, schema: FrameSchema): Array[Byte]
}