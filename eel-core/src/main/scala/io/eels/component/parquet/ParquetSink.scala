package io.eels.component.parquet

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import io.eels.schema.StructType
import io.eels.{Rec, Sink, SinkWriter}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.math.BigDecimal.RoundingMode
import scala.math.BigDecimal.RoundingMode.RoundingMode

case class ParquetWriteOptions(overwrite: Boolean = false,
                               permission: Option[FsPermission] = None,
                               dictionary: Boolean = true,
                               inheritPermissions: Option[Boolean] = None,
                               roundingMode: RoundingMode = RoundingMode.UNNECESSARY,
                               metadata: Map[String, String] = Map.empty) {

  def withOverwrite(overwrite: Boolean): ParquetWriteOptions = copy(overwrite = overwrite)
  def withDictionary(dictionary: Boolean): ParquetWriteOptions = copy(dictionary = dictionary)
  def withMetaData(map: Map[String, String]): ParquetWriteOptions = copy(metadata = map)
  def withPermission(permission: FsPermission): ParquetWriteOptions = copy(permission = permission.some)
  def withInheritPermission(inheritPermissions: Boolean): ParquetWriteOptions = copy(inheritPermissions = inheritPermissions.some)
  def withRoundingMode(mode: RoundingMode): ParquetWriteOptions = copy(roundingMode = mode)
}

case class ParquetSink(path: Path, options: ParquetWriteOptions = ParquetWriteOptions())
                      (implicit fs: FileSystem) extends Sink with Logging {

  // -- convenience methods --
  def withOverwrite(overwrite: Boolean): ParquetSink = copy(options = options.withOverwrite(overwrite))
  def withDictionary(dictionary: Boolean): ParquetSink = copy(options = options.copy(dictionary = dictionary))
  def withMetaData(map: Map[String, String]): ParquetSink = copy(options = options.copy(metadata = map))
  def withPermission(permission: FsPermission): ParquetSink = copy(options = options.copy(permission = permission.some))
  def withInheritPermission(inheritPermissions: Boolean): ParquetSink = copy(options = options.copy(inheritPermissions = inheritPermissions.some))
  def withRoundingMode(mode: RoundingMode): ParquetSink = copy(options = options.copy(roundingMode = mode))

  private def create(schema: StructType, path: Path): SinkWriter = new SinkWriter {

    if (options.overwrite && fs.exists(path))
      fs.delete(path, false)

    val writer = RowParquetWriterFn(path, schema, options.metadata, options.dictionary, options.roundingMode)

    override def write(row: Rec): Unit = {
      writer.write(row)
    }

    override def close(): Unit = {
      writer.close()
      options.permission match {
        case Some(perm) => fs.setPermission(path, perm)
        case None =>
          if (options.inheritPermissions.getOrElse(false)) {
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

object ParquetSink {
  def apply(path: String)(implicit fs: FileSystem): ParquetSink = ParquetSink(new Path(path))
}