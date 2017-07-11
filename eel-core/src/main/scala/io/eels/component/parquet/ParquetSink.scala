package io.eels.component.parquet

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import io.eels.{Row, Sink, SinkWriter}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.math.BigDecimal.RoundingMode
import scala.math.BigDecimal.RoundingMode.RoundingMode

case class ParquetSink(path: Path,
                       overwrite: Boolean = false,
                       permission: Option[FsPermission] = None,
                       dictionary: Boolean = true,
                       inheritPermissions: Option[Boolean] = None,
                       roundingMode: RoundingMode = RoundingMode.UNNECESSARY,
                       metadata: Map[String, String] = Map.empty)
                      (implicit fs: FileSystem) extends Sink with Logging {

  def withDictionary(dictionary: Boolean): ParquetSink = copy(dictionary = dictionary)
  def withMetaData(map: Map[String, String]): ParquetSink = copy(metadata = map)
  def withOverwrite(overwrite: Boolean): ParquetSink = copy(overwrite = overwrite)
  def withPermission(permission: FsPermission): ParquetSink = copy(permission = Option(permission))
  def withInheritPermission(inheritPermissions: Boolean): ParquetSink = copy(inheritPermissions = Option(inheritPermissions))
  def withRoundingMode(mode: RoundingMode): ParquetSink = copy(roundingMode = mode)
  private def create(schema: StructType, path: Path): SinkWriter = new SinkWriter {

    if (overwrite && fs.exists(path))
      fs.delete(path, false)

    val writer = RowParquetWriterFn(path, schema, metadata, dictionary, roundingMode)

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

  override def open(schema: StructType, n: Int): Seq[SinkWriter] = {
    if (n == 1) Seq(create(schema, path))
    else List.tabulate(n) { k => create(schema, new Path(path.getParent, path.getName + "_" + k)) }
  }

  override def open(schema: StructType): SinkWriter = create(schema, path)
}