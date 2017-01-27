package io.eels.component.avro

import io.eels.schema.StructType
import io.eels.{Row, Sink, SinkWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}

case class AvroSink(path: Path,
                    overwrite: Boolean = false,
                    permission: Option[FsPermission] = None,
                    inheritPermissions: Option[Boolean] = None)
                   (implicit conf: Configuration, fs: FileSystem) extends Sink {

  def withOverwrite(overwrite: Boolean): AvroSink = copy(overwrite = overwrite)
  def withPermission(permission: FsPermission): AvroSink = copy(permission = Option(permission))
  def withInheritPermission(inheritPermissions: Boolean): AvroSink = copy(inheritPermissions = Option(inheritPermissions))

  override def writer(schema: StructType): SinkWriter = new SinkWriter {

    private val writer = new AvroWriter(schema, fs.create(path, overwrite))

    override def write(row: Row): Unit = writer.write(row)

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