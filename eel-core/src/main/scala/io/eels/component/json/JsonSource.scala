package io.eels.component.json

import java.io.InputStream
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicBoolean

import com.sksamuel.exts.io.Using
import io.eels._
import io.eels.datastream.{DataStream, Publisher, Subscriber, Subscription}
import io.eels.schema.{ArrayType, DataType, Field, StructType}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.{ObjectMapper, ObjectReader}

import scala.collection.JavaConverters._

// the stream must be repeatable. That is, the input function may be called multiple times.
case class JsonSource(inputFn: () => InputStream,
                      inferrer: SchemaInferrer = StringInferrer) extends Source with Using {

  def withSchemaInferrer(inferrer: SchemaInferrer): JsonSource = copy(inferrer = inferrer)

  private val reader: ObjectReader = new ObjectMapper().reader(classOf[JsonNode])

  private def field(name: String, node: JsonNode): Field = {
    node match {
      case obj if obj.isObject => Field(name, struct(obj))
      case arr if arr.isArray => Field(name, ArrayType(datatype(name, arr.iterator().next)))
      case _ => inferrer.infer(name)
    }
  }

  private def datatype(name: String, node: JsonNode): DataType = {
    node match {
      case obj if obj.isObject => struct(obj)
      case arr if arr.isArray => ArrayType(datatype(name, arr.iterator().next))
      case _ => inferrer.infer(name).dataType
    }
  }

  private def struct(obj: JsonNode): StructType = StructType(obj.getFields.asScala.map { entry =>
    field(entry.getKey, entry.getValue)
  }.toVector)

  // we take the schema from the first entry only if there are several
  lazy val schema: StructType = using(inputFn()) { in =>
    val roots = reader.readValues[JsonNode](in)
    assert(roots.hasNext, "Cannot read schema, no data in file")
    struct(roots.next)
  }

  override def parts(): Seq[Publisher[Seq[Row]]] = List(new JsonPublisher(inputFn))

  class JsonPublisher(inputFn: () => InputStream) extends Publisher[Seq[Row]] {

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
      try {
        using(inputFn()) { input =>
          val running = new AtomicBoolean(true)
          subscriber.subscribed(Subscription.fromRunning(running))
          reader.readValues[JsonNode](input).asScala
            .takeWhile(_ => running.get)
            .map(nodeToRow)
            .grouped(DataStream.DefaultBatchSize)
            .foreach(subscriber.next)
          subscriber.completed()
        }
      } catch {
        case t: Throwable => subscriber.error(t)
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
