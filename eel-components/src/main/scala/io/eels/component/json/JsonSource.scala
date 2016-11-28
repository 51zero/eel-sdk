package io.eels.component.json

import com.sksamuel.exts.io.Using
import io.eels.schema.{Field, StructType}
import io.eels.{Part, Row, Source}
import io.reactivex.functions.Consumer
import io.reactivex.{Emitter, Flowable}
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.{ObjectMapper, ObjectReader}

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

    override def data(): Flowable[Row] = Flowable.generate(new Consumer[Emitter[Row]] {

      val input = createInputStream(path)
      val iter = reader.readValues[JsonNode](input).asScala

      override def accept(e: Emitter[Row]): Unit = {
        try {
          if (iter.hasNext) {
            val row = nodeToRow(iter.next)
            e.onNext(row)
          } else {
            e.onComplete()
          }
        } catch {
          case NonFatal(t) => e.onError(t)
        } finally {
          input.close()
        }
      }
    })
  }
}



