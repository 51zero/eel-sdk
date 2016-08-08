package io.eels.component.json

import com.sksamuel.exts.io.Using
import io.eels.schema.{Field, Schema}
import io.eels.{Part, Row, Source}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.{ObjectMapper, ObjectReader}
import rx.lang.scala.Observable

import scala.collection.JavaConverters._

case class JsonSource(path: Path)(implicit fs: FileSystem, conf: Configuration) extends Source with Using {

  private val reader: ObjectReader = new ObjectMapper().reader(classOf[JsonNode])

  private def createInputStream(path: Path): FSDataInputStream = fs.open(path)

  override def schema(): Schema = using(createInputStream(path)) { in =>
    val roots = reader.readValues[JsonNode](in)
    val node = roots.next()
    val fields = node.getFieldNames.asScala.map(name => Field(name)).toList
    Schema(fields)
  }

  override def parts(): List[Part] = List(new JsonPart(path))

  class JsonPart(val path: Path) extends Part {

    val _schema = schema()

    def nodeToRow(node: JsonNode): Row = {
      val values = node.getElements.asScala.map { it => it.getTextValue }.toList
      Row(_schema, values)
    }

    override def data(): Observable[Row] = Observable.apply { sub =>
      val input = createInputStream(path)
      try {
        sub.onStart()
        reader.readValues[JsonNode](input).asScala.foreach { it =>
          val row = nodeToRow(it)
          sub.onNext(row)
        }
      } catch {
        case t: Throwable => sub.onError(t)
      }
      sub.onCompleted()
    }
  }
}



