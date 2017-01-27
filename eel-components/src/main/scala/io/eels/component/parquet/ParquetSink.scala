package io.eels.component.parquet

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import io.eels.{Row, Sink, SinkWriter}
import org.apache.hadoop.fs.{FileSystem, Path}

case class ParquetSink(path: Path,
                       overwrite: Boolean = false,
                       metadata: Map[String, String] = Map.empty)
                      (implicit fs: FileSystem) extends Sink with Logging {

  def withMetaData(map: Map[String, String]): ParquetSink = copy(metadata = map)
  def withOverwrite(overwrite: Boolean): ParquetSink = copy(overwrite = overwrite)

  override def writer(schema: StructType): SinkWriter = new SinkWriter {

    if (overwrite && fs.exists(path))
      fs.delete(path, false)

    private val writer = ParquetWriterFn(path, schema, metadata)

    override def write(row: Row): Unit = {
      writer.write(row)
    }

    override def close(): Unit = {
      writer.close()
    }
  }
}