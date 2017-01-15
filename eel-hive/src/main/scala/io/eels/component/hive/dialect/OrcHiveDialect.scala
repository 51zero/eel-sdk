package io.eels.component.hive.dialect

import com.sksamuel.exts.Logging
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.component.orc.{OrcPart, OrcWriter}
import io.eels.component.parquet.Predicate
import io.eels.schema.StructType
import io.eels.{CloseableIterator, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}

object OrcHiveDialect extends HiveDialect with Logging {

  override def read(path: Path,
                    metastoreSchema: StructType,
                    projectionSchema: StructType,
                    predicate: Option[Predicate])
                   (implicit fs: FileSystem, conf: Configuration): CloseableIterator[Seq[Row]] = {
    new OrcPart(path).iterator()
  }

  override def writer(schema: StructType,
                      path: Path,
                      permission: Option[FsPermission],
                      metadata: Map[String, String])(implicit fs: FileSystem, conf: Configuration): HiveWriter = {

    val writer = new OrcWriter(path, schema)

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
    }
  }
}