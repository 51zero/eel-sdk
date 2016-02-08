package io.eels.component.json

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.sksamuel.scalax.io.Using
import io.eels.{Field, Column, Row, Part, FrameSchema, Source}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import scala.collection.JavaConverters._

case class JsonSource(path: Path) extends Source with Using {

  val reader = new ObjectMapper().readerFor(classOf[JsonNode])

  def createInputStream: FSDataInputStream = {
    val fs = FileSystem.get(new Configuration)
    fs.open(path)
  }

  override def schema: FrameSchema = {
    using(createInputStream) { in =>
      val roots = reader.readValues[JsonNode](in)
      val node = roots.next()
      val columns = node.fieldNames.asScala.map(Column.apply).toList
      FrameSchema(columns)
    }
  }

  override def parts: Seq[Part] = {
    val part = new Part {

      override def iterator: Iterator[Row] = new Iterator[Row] {

        val in = createInputStream
        val roots = reader.readValues[JsonNode](in)

        def close(): Unit = in.close()

        val iter = roots.asScala
        override def hasNext: Boolean = iter.hasNext
        override def next(): Row = nodeToRow(iter.next)

        def nodeToRow(node: JsonNode): Row = {
          val columns = node.fieldNames.asScala.map(Column.apply).toList
          val fields = node.elements.asScala.map(node => Field(node.textValue)).toList
          Row(columns, fields)
        }
      }
    }
    Seq(part)
  }
}
