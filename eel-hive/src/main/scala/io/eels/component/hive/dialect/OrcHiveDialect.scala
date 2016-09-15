package io.eels.component.hive.dialect

import com.sksamuel.exts.Logging
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.component.orc.{OrcReader, OrcWriter}
import io.eels.schema.Schema
import io.eels.Row
import io.eels.component.parquet.Predicate
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.io.orc.{OrcInputFormat, OrcOutputFormat, OrcSerde}
import rx.lang.scala.Observable

object OrcHiveDialect extends HiveDialect with Logging {

  override def read(path: Path,
                    metastoreSchema: Schema,
                    projectionSchema: Schema,
                    predicate: Option[Predicate])
                   (implicit fs: FileSystem, conf: Configuration): Observable[Row] = new OrcReader(path).rows()

  override def writer(schema: Schema,
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