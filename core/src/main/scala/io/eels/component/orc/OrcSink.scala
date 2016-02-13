package io.eels.component.orc

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{FrameSchema, Row, Sink, Writer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

case class OrcSink(path: Path) extends Sink with StrictLogging {
  logger.debug(s"Created orc sink from $path")

  override def writer: Writer = new Writer {

    var writer: org.apache.hadoop.hive.ql.io.orc.Writer = null

    private def createWriter(schema: FrameSchema): Unit = {
      if (writer == null) {
        logger.debug(s"Creating orc writer $schema")
        val inspector = ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector
        )
        writer = OrcFile.createWriter(path, OrcFile.writerOptions(new Configuration).inspector(inspector))
      }
    }

    override def close(): Unit = if (writer != null) writer.close()

    override def write(row: Row): Unit = {
      createWriter(FrameSchema(row.columns))
      writer.addRow(row.fields.map(_.value).toArray)
    }
  }
}
