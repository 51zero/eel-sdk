package io.eels.component.orc

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{InternalRow, Schema, Sink, Writer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

case class OrcSink(path: Path) extends Sink with StrictLogging {
  self =>

  logger.debug(s"Created orc sink from $path")

  override def writer: Writer = new Writer {

    var writer: org.apache.hadoop.hive.ql.io.orc.Writer = null

    private def createWriter(schema: Schema): Unit = {
      if (writer == null) {
        logger.debug(s"Creating orc writer $schema")
        val inspector = ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector
        )
        writer = OrcFile.createWriter(path, OrcFile.writerOptions(new Configuration).inspector(inspector))
        writer.addRow(schema.columnNames.toArray)
      }
    }

    override def close(): Unit = if (writer != null) writer.close()

    override def write(row: InternalRow, schema: Schema): Unit = {
      self.synchronized {
        createWriter(schema)
        writer.addRow(row.toArray)
      }
    }
  }
}
