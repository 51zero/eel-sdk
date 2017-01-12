package io.eels.component.orc

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import io.eels.{Row, Sink, SinkWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

case class OrcSink(path: Path)
                  (implicit conf: Configuration) extends Sink with Logging {

  override def writer(schema: StructType): SinkWriter = new SinkWriter {
    val writer = new OrcWriter(path, schema)
    override def write(row: Row): Unit = writer.write(row)
    override def close(): Unit = writer.close()
  }
}


