package io.eels.component.json

import com.sksamuel.exts.io.Using
import io.eels._
import io.eels.schema.{Field, StructType}
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.{ObjectMapper, ObjectReader}

import scala.collection.JavaConverters._

case class JsonSource(path: Path)(implicit fs: FileSystem) extends Source with Using {
  require(fs.exists(path), s"$path must exist")

  private val reader: ObjectReader = new ObjectMapper().reader(classOf[JsonNode])

  private def createInputStream(path: Path): FSDataInputStream = fs.open(path)

  override def schema(): StructType = using(createInputStream(path)) { in =>
    val roots = reader.readValues[JsonNode](in)
    assert(roots.hasNext, "Cannot read schema, no data in file")
    val node = roots.next()
    val fields = node.getFieldNames.asScala.map(name => Field(name)).toList
    StructType(fields)
  }

  override def parts(): List[Part] = List(new JsonPart(path))

  class JsonPart(val path: Path) extends Part {

    val _schema = schema()

    def nodeToRow(node: JsonNode): Row = {
      val values = node.getElements.asScala.map { it => it.getTextValue }.toList
      Row(_schema, values)
    }
    /**
      * Returns the data contained in this part in the form of an iterator. This function should return a new
      * iterator on each invocation. The iterator can be lazily initialized to the first read if required.
      */
    override def iterator(): CloseableIterator[List[Row]] = new CloseableIterator[List[Row]] {

      val input = createInputStream(path)
      val iter = reader.readValues[JsonNode](input).asScala.map(nodeToRow).grouped(100).withPartial(true)
      var closed = false

      override def next(): List[Row] = iter.next()
      override def hasNext(): Boolean = !closed && iter.hasNext

      override def close(): Unit = {
        closed = true
        input.close()
      }
    }
  }
}



