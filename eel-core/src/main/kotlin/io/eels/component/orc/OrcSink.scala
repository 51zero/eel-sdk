package io.eels.component.orc

import io.eels.{Schema, Sink, SinkWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

case class OrcSink(path: Path) extends Sink with StrictLogging {
  override def writer(schema: Schema): SinkWriter = new OrcSinkWriter(schema, path)
}

class OrcSinkWriter(schema: Schema, path: Path) extends SinkWriter with StrictLogging {
  self =>
  logger.debug(s"Creating orc writer $schema")

  val inspector = ObjectInspectorFactory.getStandardListObjectInspector(
    PrimitiveObjectInspectorFactory.javaStringObjectInspector
  )
  val writer = OrcFile.createWriter(path, OrcFile.writerOptions(new Configuration).inspector(inspector))
  writer.addRow(schema.columnNames.toArray)

  override def write(row: InternalRow): Unit = {
    self.synchronized {
      writer.addRow(row.toArray)
    }
  }

  override def close(): Unit = writer.close()
}
