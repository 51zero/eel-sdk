package io.eels.component.orc

import com.sksamuel.exts.io.Using
import io.eels.schema.Schema
import io.eels.{Part, Row, Source}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.io.Text
import rx.lang.scala.Observable
import scala.collection.JavaConverters._

case class OrcSource(path: Path)(implicit fs: FileSystem) extends Source with Using {

  override def parts(): List[Part] = List(new OrcPart(path, fs))

  override def schema(): Schema = {
    val reader = OrcFns.createOrcReader(path, fs)
    val schema = OrcFns.orcSchemaFromReader(reader)
    reader.close()
    schema
  }

  class OrcPart(path: Path, fs: FileSystem) extends Part {
    override def data(): Observable[Row] = {
      Observable { it =>

        try {
          val reader = OrcFile.createReader(fs, path).rows()
          val schema = OrcFns.orcSchemaFromReader(reader)

          it.onStart()

          while (!it.isUnsubscribed && reader.hasNext()) {
            val next = reader.next(null)
            val values = next.asInstanceOf[java.util.List[Any]].asScala
            val normalizedValues = values.map {
              case it: Text => it.toString()
              case it => it
            }.toVector
            val row = Row(schema, normalizedValues)
            it.onNext(row)
          }

          it.onCompleted()
          reader.close()
        } catch {
          case t: Throwable =>
          it.onError(t)
        }
      }
    }
  }
}