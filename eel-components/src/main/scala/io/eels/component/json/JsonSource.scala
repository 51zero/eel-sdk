package io.eels.component.json

import java.util.function.Consumer

import com.sksamuel.exts.io.Using
import io.eels.schema.{Field, StructType}
import io.eels.{Part, Row, Source}
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.{ObjectMapper, ObjectReader}
import reactor.core.publisher.{Flux, FluxSink}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

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

    override def data(): Flux[Row] = Flux.create(new Consumer[FluxSink[Row]] {
      override def accept(sink: FluxSink[Row]): Unit = {

        val input = createInputStream(path)
        val iter = reader.readValues[JsonNode](input).asScala

        try {
          while (iter.hasNext) {
            val row = nodeToRow(iter.next)
            sink.next(row)
          }
          sink.complete()
        } catch {
          case NonFatal(error) =>
            logger.warn("Could not read file", error)
            sink.error(error)
        } finally {
          input.close()
        }
      }
    }, FluxSink.OverflowStrategy.BUFFER)
  }
}



