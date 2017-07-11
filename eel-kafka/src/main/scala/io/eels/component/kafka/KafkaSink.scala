package io.eels.component.kafka

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import io.eels.{Row, SinkWriter, Sink}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * A KafkaPartitioner is used to generate an optional partition key
  * for each final value. If the KafkaPartitioner returns None then
  * no partition is used when the KafkaSink proicuces a kafka message.
  *
  * By default a no-op is supplied which will be used if another
  * more specific partition is not provided.
  */
trait KafkaPartitioner[+V] {
  def partition(row: Row): Option[Int]
}

object KafkaPartitioner {
  implicit object NoopKafkaPartitioner extends KafkaPartitioner[Nothing] {
    def partition(row: Row): Option[Int] = None
  }
}

/**
  * An instance of KafkaKeyGen is used to generate a key from the row to
  * be used by the KafkaSink when it produces a kafka message.
  *
  * If the key to be used is an Int or a String then a default HashCodeKeyGen
  * is provided which will use the row objects hashcode if no more specific
  * KafkaKeyGen is provided.
  *
  * @tparam K the key type used by the KafkaProducer passed into the KafkaSink.
  */
trait KafkaKeyGen[K] {
  def gen(row: Row): K
}

object KafkaKeyGen {
  implicit object HashCodeKeyGen extends KafkaKeyGen[Int] {
    override def gen(row: Row): Int = row.hashCode()
  }
  implicit object StringKeyGen extends KafkaKeyGen[String] {
    override def gen(row: Row): String = row.hashCode.toString
  }
}

/**
  * An instance of KafkaRowConverter is used to convert an eel-sdk Row into another
  * type that is compatible with the KafkaProducer used in the KafkaSink.
  *
  * Note that the KafkaSink can write to a KafkaProducer[K, Row] in which case the
  * KafkaRowConverter can be a no-op, which will be supplied automatically.
  *
  * @tparam V the final type that will be written to the KafkaProducer
  */
trait KafkaRowConverter[V] {
  def convert(row: Row): V
}

object KafkaRowConverter {
  implicit object NoopRowConverter extends KafkaRowConverter[Row] {
    override def convert(row: Row): Row = row
  }
}

case class KafkaSink[K, V](topic: String,
                           producer: KafkaProducer[K, V])
                          (implicit partitioner: KafkaPartitioner[V],
                           converter: KafkaRowConverter[V],
                           keygen: KafkaKeyGen[K]) extends Sink with Logging {

  def open(schema: StructType): SinkWriter = {

    new SinkWriter {
      override def write(row: Row): Unit = {
        val key = keygen.gen(row)
        val value = converter.convert(row)
        val record = partitioner.partition(row) match {
          case Some(part) => new ProducerRecord[K, V](topic, part, key, value)
          case _ => new ProducerRecord[K, V](topic, key, value)
        }
        logger.debug(s"Sending record $record")
        producer.send(record)
        producer.flush()
      }
      override def close(): Unit = producer.close()
    }
  }
}