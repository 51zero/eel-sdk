package io.eels.component.hive.dialect

import com.sksamuel.exts.Logging
import io.eels.{Flow, Predicate, Row}
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.component.orc.{OrcPart, OrcSinkConfig, OrcWriter}
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}

object OrcHiveDialect extends HiveDialect with Logging {

  override def read(path: Path,
                    metastoreSchema: StructType,
                    projectionSchema: StructType,
                    predicate: Option[Predicate])
                   (implicit fs: FileSystem, conf: Configuration): Flow = {
    new OrcPart(path, projectionSchema.fieldNames(), predicate).open()
  }

  override def writer(schema: StructType,
                      path: Path,
                      permission: Option[FsPermission],
                      metadata: Map[String, String])(implicit fs: FileSystem, conf: Configuration): HiveWriter = {

    val path_x = path
    val writer = new OrcWriter(path, schema, Nil, None, OrcSinkConfig())

    new HiveWriter {

      override def write(row: Row): Unit = {
        require(row.values.nonEmpty, "Attempting to write an empty row")
        writer.write(row)
      }

      override def close(): Unit = {
        writer.close()
        permission.foreach(fs.setPermission(path, _))
      }

      override def records: Int = writer.records
      override def path: Path = path_x
    }
  }
}