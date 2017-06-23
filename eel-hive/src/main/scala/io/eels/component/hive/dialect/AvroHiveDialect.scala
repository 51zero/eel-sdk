package io.eels.component.hive.dialect

import com.sksamuel.exts.Logging
import io.eels.{Channel, Predicate, Row}
import io.eels.component.avro.{AvroSourcePart, AvroWriter}
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}

object AvroHiveDialect extends HiveDialect with Logging {

  override def read(path: Path,
                    metastoreSchema: StructType,
                    projectionSchema: StructType,
                    predicate: Option[Predicate])
                   (implicit fs: FileSystem, conf: Configuration): Channel[Row] = {
    AvroSourcePart(path).channel()
  }

  override def writer(schema: StructType,
                      path: Path,
                      permission: Option[FsPermission],
                      metadata: Map[String, String])
                     (implicit fs: FileSystem, conf: Configuration): HiveWriter = {
    val path_x = path
    new HiveWriter {

      private val out = fs.create(path)
      private val writer = new AvroWriter(schema, out)


      override def write(row: Row): Unit = writer.write(row)

      override def close(): Unit = {
        writer.close()
        out.close()
      }

      override def records: Int = writer.records
      override def path: Path = path_x
    }
  }
}
