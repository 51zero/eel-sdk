package io.eels.component.parquet

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import io.eels.{Row, Sink, SinkWriter}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}

case class ParquetSink(path: Path,
                       overwrite: Boolean = false,
                       permission: Option[FsPermission] = None,
                       inheritPermissions: Option[Boolean] = None,
                       metadata: Map[String, String] = Map.empty)
                      (implicit fs: FileSystem) extends Sink with Logging {

  def withMetaData(map: Map[String, String]): ParquetSink = copy(metadata = map)
  def withOverwrite(overwrite: Boolean): ParquetSink = copy(overwrite = overwrite)
  def withPermission(permission: FsPermission): ParquetSink = copy(permission = Option(permission))
  def withInheritPermission(inheritPermissions: Boolean): ParquetSink = copy(inheritPermissions = Option(inheritPermissions))

  override def writer(schema: StructType): SinkWriter = new SinkWriter {

    if (overwrite && fs.exists(path))
      fs.delete(path, false)

    private val writer = ParquetWriterFn(path, schema, metadata)

    override def write(row: Row): Unit = {
      writer.write(row)
    }

    override def close(): Unit = {
      writer.close()
      permission match {
        case Some(perm) => fs.setPermission(path, perm)
        case None =>
          if (inheritPermissions.getOrElse(false)) {
            val permission = fs.getFileStatus(path.getParent).getPermission
            fs.setPermission(path, permission)
          }
      }
    }
  }
}