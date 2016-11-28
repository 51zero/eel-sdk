package io.eels.component.hive.dialect

import com.sksamuel.exts.Logging
import io.eels.Row
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.component.orc.{OrcReader, OrcWriter}
import io.eels.component.parquet.Predicate
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import reactor.core.publisher.Flux

object OrcHiveDialect extends HiveDialect with Logging {

  override def read(path: Path,
                    metastoreSchema: StructType,
                    projectionSchema: StructType,
                    predicate: Option[Predicate])
                   (implicit fs: FileSystem, conf: Configuration): Flux[Row] = new OrcReader(path).rows()

  override def writer(schema: StructType,
                      path: Path,
                      permission: Option[FsPermission])
                     (implicit fs: FileSystem, conf: Configuration): HiveWriter = new HiveWriter {

    val writer = new OrcWriter(path, schema)

    override def write(row: Row) {
      require(row.values.nonEmpty, "Attempting to write an empty row")
      writer.write(row)
    }

    override def close() = {
      writer.close()
      permission.foreach(fs.setPermission(path, _))
    }
  }
}