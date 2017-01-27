package io.eels.component.orc

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import io.eels.schema.StructType
import io.eels.{Row, Sink, SinkWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}

case class OrcSink(path: Path,
                   overwrite: Boolean = false,
                   bloomFilterColumns: Seq[String] = Nil,
                   permission: Option[FsPermission] = None,
                   inheritPermissions: Option[Boolean] = None,
                   rowIndexStride: Option[Int] = None)
                  (implicit fs: FileSystem, conf: Configuration) extends Sink with Logging {

  def withBloomFilterColumns(bloomFilterColumns: Seq[String]): OrcSink = copy(bloomFilterColumns = bloomFilterColumns)
  def withRowIndexStride(stride: Int): OrcSink = copy(rowIndexStride = stride.some)
  def withOverwrite(overwrite: Boolean): OrcSink = copy(overwrite = overwrite)
  def withPermission(permission: FsPermission): OrcSink = copy(permission = Option(permission))
  def withInheritPermission(inheritPermissions: Boolean): OrcSink = copy(inheritPermissions = Option(inheritPermissions))

  override def writer(schema: StructType): SinkWriter = new SinkWriter {

    if (overwrite && fs.exists(path))
      fs.delete(path, false)

    val writer = new OrcWriter(path, schema, bloomFilterColumns, rowIndexStride)

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