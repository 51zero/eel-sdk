package io.eels.component.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import io.eels.{Schema, InternalRow}

import scala.collection.JavaConverters._

object JsonKafkaDeserializer extends KafkaDeserializer {
  val mapper = new ObjectMapper
  override def apply(bytes: Array[Byte]): InternalRow = {
    val node = mapper.readTree(bytes)
    //    val columns = node.fieldNames.asScala.map(Column.apply).toList
    //    val fields = node.fieldNames.asScala.map { name => Field(node.get(name).textValue) }.toList
    node.elements.asScala.map(_.textValue).toList
  }
}

object JsonKafkaSerializer extends KafkaSerializer {
  val mapper = new ObjectMapper
  override def apply(row: InternalRow, schema: Schema): Array[Byte] = mapper.writeValueAsBytes(row)
}