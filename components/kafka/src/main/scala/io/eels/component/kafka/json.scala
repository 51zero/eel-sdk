package io.eels.component.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import io.eels.{Column, Field, Row}
import scala.collection.JavaConverters._

object JsonKafkaDeserializer extends KafkaDeserializer {
  val mapper = new ObjectMapper
  override def apply(bytes: Array[Byte]): Row = {
    val node = mapper.readTree(bytes)
    val columns = node.fieldNames.asScala.map(Column.apply).toList
    val fields = node.fieldNames.asScala.map { name => Field(node.get(name).textValue) }.toList
    Row(columns, fields)
  }
}

object JsonKafkaSerializer extends KafkaSerializer {
  val mapper = new ObjectMapper
  override def apply(row: Row): Array[Byte] = mapper.writeValueAsBytes(row.toMap)
}