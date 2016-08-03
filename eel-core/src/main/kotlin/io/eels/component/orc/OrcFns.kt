package io.eels.component.orc

import io.eels.schema.Field
import io.eels.schema.Schema
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.hive.ql.io.orc.RecordReader

object OrcFns {

  fun createOrcReader(path: Path, fs: FileSystem): RecordReader = OrcFile.createReader(fs, path).rows()

  fun orcSchemaFromReader(reader: RecordReader): Schema {
    val fields = reader.next(null) as List<Any?>
    val columns = fields.map { Field(it.toString()) }
    return Schema(columns)
  }
}