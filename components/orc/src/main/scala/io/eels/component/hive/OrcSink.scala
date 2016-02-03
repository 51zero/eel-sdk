package io.eels.component.hive

import io.eels.{FrameSchema, Row, Sink, Writer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

case class OrcSink(path: Path) extends Sink {
  override def writer: Writer = new Writer {

    var writer: org.apache.hadoop.hive.ql.io.orc.Writer = null

    override def close(): Unit = writer.close()

    override def write(row: Row): Unit = {
      createWriter(FrameSchema(row.columns))
      writer.addRow(row.fields.map(_.value).toArray)
    }

    def createWriter(schema: FrameSchema): Unit = {
      if (writer == null) {
        val inspector = ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector
        )
        writer = OrcFile.createWriter(path, OrcFile.writerOptions(new Configuration).inspector(inspector))
      }
    }
  }
}
