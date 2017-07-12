package io.eels.component.json

import java.io.InputStream
import java.nio.file.Files

import com.sksamuel.exts.io.Using
import io.eels._
import io.eels.datastream.Subscriber
import io.eels.schema.{Field, StructType}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.{ObjectMapper, ObjectReader}

import scala.collection.JavaConverters._

// the stream must be repeatable. That is, the input function may be called multiple times.
case class JsonSource(inputFn: () => InputStream) extends Source with Using {

  private val reader: ObjectReader = new ObjectMapper().reader(classOf[JsonNode])

  lazy val schema: StructType = using(inputFn()) { in =>
    val roots = reader.readValues[JsonNode](in)
    assert(roots.hasNext, "Cannot read schema, no data in file")
    val node = roots.next()
    val fields = node.getFieldNames.asScala.map(name => Field(name)).toList
    StructType(fields)
  }

  override def parts(): List[Part] = List(new JsonPart(inputFn))

  class JsonPart(inputFn: () => InputStream) extends Part {

    private def nodeToValue(node: JsonNode): Any = {
      if (node.isArray) {
        node.getElements.asScala.map(nodeToValue).toVector
      } else if (node.isObject) {
        node.getFields.asScala.map { entry =>
          entry.getKey -> nodeToValue(entry.getValue)
        }.toMap
      } else if (node.isBinary) {
        node.getBinaryValue
      } else if (node.isBigInteger) {
        node.getBigIntegerValue
      } else if (node.isBoolean) {
        node.getBooleanValue
      } else if (node.isTextual) {
        node.getTextValue
      } else if (node.isLong) {
        node.getLongValue
      } else if (node.isInt) {
        node.getIntValue
      } else if (node.isDouble) {
        node.getDoubleValue
      }
    }

    private def nodeToRow(node: JsonNode): Row = {
      val values = node.getElements.asScala.map(nodeToValue).toArray
      Row(schema, values)
    }

    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      using(inputFn()) { input =>
        try {
          val iterator = reader.readValues[JsonNode](input).asScala.map(nodeToRow)
          iterator.grouped(1000).foreach(subscriber.next)
          subscriber.completed()
        } catch {
          case t: Throwable => subscriber.error(t)
        }
      }
    }
  }
}

object JsonSource {

  def apply(path: Path)(implicit fs: FileSystem): JsonSource = {
    require(fs.exists(path), s"$path must exist")
    JsonSource(() => fs.open(path))
  }

  def apply(path: java.nio.file.Path): JsonSource = {
    require(path.toFile.exists, s"$path must exist")
    JsonSource(() => Files.newInputStream(path))
  }
}
