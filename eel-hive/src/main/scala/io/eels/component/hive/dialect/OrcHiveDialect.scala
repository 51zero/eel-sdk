package io.eels.component.hive.dialect

import com.sksamuel.exts.Logging
import io.eels.component.hive.{HiveDialect, HiveOutputStream, Publisher}
import io.eels.component.orc.{OrcPart, OrcSinkConfig, OrcWriter}
import io.eels.datastream.Subscriber
import io.eels.schema.StructType
import io.eels.{Predicate, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}

class OrcHiveDialect extends HiveDialect with Logging {

  override def input(path: Path,
                     metastoreSchema: StructType,
                     projectionSchema: StructType,
                     predicate: Option[Predicate])
                    (implicit fs: FileSystem, conf: Configuration): Publisher[Seq[Row]] = new Publisher[Seq[Row]] {
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      new OrcPart(path, projectionSchema.fieldNames(), predicate).subscribe(subscriber)
    }
  }

  override def output(schema: StructType,
                      path: Path,
                      permission: Option[FsPermission],
                      metadata: Map[String, String])(implicit fs: FileSystem, conf: Configuration): HiveOutputStream = {

    val path_x = path
    val writer = new OrcWriter(path, schema, Nil, None, OrcSinkConfig())

    new HiveOutputStream {

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