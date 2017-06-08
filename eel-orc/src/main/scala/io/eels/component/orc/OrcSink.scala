package io.eels.component.orc

import com.sksamuel.exts.Logging
import com.sksamuel.exts.config.ConfigSupport
import com.typesafe.config.ConfigFactory
import io.eels.schema.StructType
import io.eels.{Row, Sink, SinkWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.orc.OrcFile.{CompressionStrategy, EncodingStrategy}
import org.apache.orc.OrcProto.CompressionKind
import com.sksamuel.exts.OptionImplicits._

case class OrcSinkConfig(compressionKind: CompressionKind,
                         compressionStrategy: CompressionStrategy,
                         compressionBufferSize: Option[Int],
                         encodingStrategy: Option[EncodingStrategy]) {
  def withCompressionKind(kind: CompressionKind): OrcSinkConfig = copy(compressionKind = kind)
  def withCompressionStrategy(strategy: CompressionStrategy): OrcSinkConfig = copy(compressionStrategy = strategy)
  def withCompressionBufferSize(size: Int): OrcSinkConfig = copy(compressionBufferSize = size.some)
  def withEncodingStrategy(strategy: EncodingStrategy): OrcSinkConfig = copy(encodingStrategy = strategy.some)
}

object OrcSinkConfig extends ConfigSupport {

  // creates a config from the typesafe reference.confs
  def apply(): OrcSinkConfig = {
    val config = ConfigFactory.load()
    OrcSinkConfig(
      CompressionKind valueOf config.getString("eel.orc.writer.compression-kind"),
      CompressionStrategy valueOf config.getString("eel.orc.writer.compression-strategy"),
      config.getIntOpt("eel.orc.writer.compression-buffer-size"),
      config.getStringOpt("eel.orc.writer.encoding-strategy").map(EncodingStrategy.valueOf)
    )
  }
}

case class OrcSink(path: Path,
                   overwrite: Boolean = false,
                   bloomFilterColumns: Seq[String] = Nil,
                   permission: Option[FsPermission] = None,
                   inheritPermissions: Option[Boolean] = None,
                   rowIndexStride: Option[Int] = None,
                   config: OrcSinkConfig = OrcSinkConfig())
                  (implicit fs: FileSystem, conf: Configuration) extends Sink with Logging {

  def withBloomFilterColumns(bloomFilterColumns: Seq[String]): OrcSink = copy(bloomFilterColumns = bloomFilterColumns)
  def withRowIndexStride(stride: Int): OrcSink = copy(rowIndexStride = stride.some)
  def withOverwrite(overwrite: Boolean): OrcSink = copy(overwrite = overwrite)
  def withPermission(permission: FsPermission): OrcSink = copy(permission = Option(permission))
  def withInheritPermission(inheritPermissions: Boolean): OrcSink = copy(inheritPermissions = Option(inheritPermissions))

  override def writer(schema: StructType): SinkWriter = new SinkWriter {

    if (overwrite && fs.exists(path))
      fs.delete(path, false)

    val writer = new OrcWriter(path, schema, bloomFilterColumns, rowIndexStride, config)

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