package io.eels.component.orc

import io.eels.Row
import io.eels.Source
import io.eels.component.Part
import io.eels.component.Using
import io.eels.schema.Schema
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.io.Text
import rx.Observable

class OrcSource(val path: Path, val fs: FileSystem) : Source, Using {

  override fun parts(): List<Part> = listOf(OrcPart(path, fs))

  override fun schema(): Schema {
    val reader = OrcFns.createOrcReader(path, fs)
    val schema = OrcFns.orcSchemaFromReader(reader)
    reader.close()
    return schema
  }

  class OrcPart(val path: Path, val fs: FileSystem) : Part {
    override fun data(): Observable<Row> {
      return Observable.create {

        try {
          val reader = OrcFile.createReader(fs, path).rows()
          val schema = OrcFns.orcSchemaFromReader(reader)

          it.onStart()

          while (!it.isUnsubscribed && reader.hasNext()) {
            val values = reader.next(null) as List<Any?>
            val normalizedValues = values.map {
              when (it) {
                is Text -> it.toString()
                else -> it
              }
            }
            val row = Row(schema, normalizedValues)
            it.onNext(row)
          }

          it.onCompleted()
          reader.close()
        } catch (t: Throwable) {
          it.onError(t)
        }
      }
    }
  }
}