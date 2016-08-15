package io.eels.component.orc

import io.eels.schema.Field
import io.eels.schema.Schema
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.hive.ql.io.orc.RecordReader
import scala.collection.JavaConverters._

object OrcFns {

  def createOrcReader(path: Path, fs: FileSystem): RecordReader = OrcFile.createReader(fs, path).rows()

  def orcSchemaFromReader(reader: RecordReader): Schema = {
    val fields = reader.next(null).asInstanceOf[java.util.List[Any]].asScala
    Schema(
      fields.map { it => new Field(it.toString()) }.toList
    )
  }
}