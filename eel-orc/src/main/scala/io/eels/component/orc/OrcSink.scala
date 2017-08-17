package io.eels.component.orc

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import com.sksamuel.exts.config.ConfigSupport
import com.typesafe.config.ConfigFactory
import io.eels.schema.StructType
import io.eels.{Row, Sink, SinkWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.orc.OrcFile.{CompressionStrategy, EncodingStrategy}
import org.apache.orc.OrcProto.CompressionKind

case class OrcWriteOptions(overwrite: Boolean = false,
                           compressionKind: CompressionKind,
                           compressionStrategy: CompressionStrategy,
                           compressionBufferSize: Option[Int],
                           encodingStrategy: Option[EncodingStrategy],
                           bloomFilterColumns: Seq[String] = Nil,
                           permission: Option[FsPermission] = None,
                           inheritPermissions: Option[Boolean] = None,
                           rowIndexStride: Option[Int] = None) {
  def withCompressionKind(kind: CompressionKind): OrcWriteOptions = copy(compressionKind = kind)
  def withCompressionStrategy(strategy: CompressionStrategy): OrcWriteOptions = copy(compressionStrategy = strategy)
  def withCompressionBufferSize(size: Int): OrcWriteOptions = copy(compressionBufferSize = size.some)
  def withEncodingStrategy(strategy: EncodingStrategy): OrcWriteOptions = copy(encodingStrategy = strategy.some)
  def withBloomFilterColumns(bloomFilterColumns: Seq[String]): OrcWriteOptions = copy(bloomFilterColumns = bloomFilterColumns)
  def withRowIndexStride(stride: Int): OrcWriteOptions = copy(rowIndexStride = stride.some)
  def withOverwrite(overwrite: Boolean): OrcWriteOptions = copy(overwrite = overwrite)
  def withPermission(permission: FsPermission): OrcWriteOptions = copy(permission = permission.some)
  def withInheritPermission(inheritPermissions: Boolean): OrcWriteOptions = copy(inheritPermissions = inheritPermissions.some)
}

object OrcWriteOptions extends ConfigSupport {

  // creates a config from the typesafe reference.confs
  def apply(): OrcWriteOptions = {
    val config = ConfigFactory.load()
    OrcWriteOptions(
      false,
      CompressionKind valueOf config.getString("eel.orc.writer.compression-kind"),
      CompressionStrategy valueOf config.getString("eel.orc.writer.compression-strategy"),
      config.getIntOpt("eel.orc.writer.compression-buffer-size"),
      config.getStringOpt("eel.orc.writer.encoding-strategy").map(EncodingStrategy.valueOf)
    )
  }
}

case class OrcSink(path: Path, options: OrcWriteOptions = OrcWriteOptions())
                  (implicit fs: FileSystem, conf: Configuration) extends Sink with Logging {

  // -- convenience options --
  def withCompressionKind(kind: CompressionKind): OrcSink = copy(options = options.copy(compressionKind = kind))
  def withCompressionStrategy(strategy: CompressionStrategy): OrcSink = copy(options = options.copy(compressionStrategy = strategy))
  def withCompressionBufferSize(size: Int): OrcSink = copy(options = options.copy(compressionBufferSize = size.some))
  def withEncodingStrategy(strategy: EncodingStrategy): OrcSink = copy(options = options.copy(encodingStrategy = strategy.some))
  def withBloomFilterColumns(bloomFilterColumns: Seq[String]): OrcSink = copy(options = options.copy(bloomFilterColumns = bloomFilterColumns))
  def withRowIndexStride(stride: Int): OrcSink = copy(options = options.copy(rowIndexStride = stride.some))
  def withOverwrite(overwrite: Boolean): OrcSink = copy(options = options.copy(overwrite = overwrite))
  def withPermission(permission: FsPermission): OrcSink = copy(options = options.copy(permission = permission.some))
  def withInheritPermission(inheritPermissions: Boolean): OrcSink = copy(options = options.copy(inheritPermissions = inheritPermissions.some))

  override def open(schema: StructType, n: Int): Seq[SinkWriter] = {
    if (n == 1) Seq(create(schema, path))
    else List.tabulate(n) { k => create(schema, new Path(path.getParent, path.getName + "_" + k)) }
  }

  override def open(schema: StructType): SinkWriter = create(schema, path)

  private def create(schema: StructType, path: Path): SinkWriter = new SinkWriter {

    if (options.overwrite && fs.exists(path))
      fs.delete(path, false)

    val writer = new OrcWriter(path, schema, options)

    override def write(row: Row): Unit = writer.write(row)
    
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
}