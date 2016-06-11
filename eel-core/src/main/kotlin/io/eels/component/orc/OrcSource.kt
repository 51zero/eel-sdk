package io.eels.component.orc

import io.eels.Column
import io.eels.Row
import io.eels.Schema
import io.eels.Source
import io.eels.component.Part
import io.eels.component.Using
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.hive.ql.io.orc.RecordReader
import rx.Observable

class OrcSource(val path: Path, val fs: FileSystem) : Source, Using {

  override fun parts(): List<Part> = listOf(OrcPart(path, fs))

  override fun schema(): Schema {
    val reader = createOrcReader(path, fs)
    val schema = orcSchemaFromReader(reader)
    reader.close()
    return schema
  }
}

class OrcPart(val path: Path, val fs: FileSystem) : Part {
  override fun data(): Observable<Row> {
    return Observable.create {

      val reader = OrcFile.createReader(fs, path).rows()
      val schema = orcSchemaFromReader(reader)

      it.onStart()

      while (!it.isUnsubscribed && reader.hasNext()) {
        val values = reader.next(null) as List<Any?>
        val row = Row(schema, values)
        it.onNext(row)
      }

      it.onCompleted()
      reader.close()
    }
  }
}

fun createOrcReader(path: Path, fs: FileSystem): RecordReader = OrcFile.createReader(fs, path).rows()

fun orcSchemaFromReader(reader: RecordReader): Schema {
  val fields = reader.next(null) as List<Any?>
  val columns = fields.map { Column(it.toString()) }
  return Schema(columns)
}