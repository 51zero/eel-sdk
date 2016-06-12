package io.eels.component.json

import io.eels._
import io.eels.schema.{Field, Schema}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

case class JsonSource(path: Path) extends Source with Using {

  val reader = new ObjectMapper().readerFor(classOf[JsonNode])

  private def createInputStream(path: Path): FSDataInputStream = {
    val fs = FileSystem.get(new Configuration)
    fs.open(path)
  }

  override def schema: Schema = {
    using(createInputStream(path)) { in =>
      val roots = reader.readValues[JsonNode](in)
      val node = roots.next()
      val columns = node.fieldNames.asScala.map(Field.apply).toList
      Schema(columns)
    }
  }

  class JsonPart(path: Path) extends Part {
    override def reader: SourceReader = new JsonSourceReader(path)
  }

  class JsonSourceReader(path: Path) extends SourceReader {

    val in = createInputStream(path)
    val reader = new ObjectMapper().readerFor(classOf[JsonNode])
    val roots = reader.readValues[JsonNode](in)
    val iter = roots.asScala

    def nodeToRow(node: JsonNode): InternalRow = {
      //val columns = node.fieldNames.asScala.map(Column.apply).toList
      //val fields = node.fieldNames.asScala.map { name => Field(node.get(name).textValue) }.toList
      //Row(columns, fields)
      node.elements.asScala.map(_.textValue).toList
    }

    override def close(): Unit = in.close()
    override def iterator: Iterator[InternalRow] = new Iterator[InternalRow] {
      override def hasNext: Boolean = iter.hasNext
      override def next(): InternalRow = nodeToRow(iter.next)
    }
  }

  override def parts: Seq[Part] = Seq(new JsonPart(path))
}


