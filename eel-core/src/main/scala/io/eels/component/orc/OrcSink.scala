package io.eels.component.orc

import com.sksamuel.exts.Logging
import io.eels.{Row, Sink, SinkWriter}
import io.eels.schema.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.{OrcFile, Writer}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorFactory, StandardListObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

class OrcSink(val path: Path) extends Sink with Logging {

  override def writer(schema: Schema): SinkWriter = new OrcSinkWriter(schema, path)

  class OrcSinkWriter(schema: Schema, path: Path) extends SinkWriter with Logging {
    logger.debug(s"Creating orc writer $schema")

    val inspector: StandardListObjectInspector = ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaStringObjectInspector
    )

    val writer: Writer = OrcFile.createWriter(path, OrcFile.writerOptions(new Configuration()).inspector(inspector))
    writer.addRow(schema.fieldNames().toArray)

    override def write(row: Row): Unit = {
      this.synchronized {
        writer.addRow(row.values)
      }
    }

    override def close(): Unit = writer.close()
  }
}


