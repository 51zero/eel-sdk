package io.eels.component.orc

import io.eels.util.Logging
import io.eels.Row
import io.eels.schema.Schema
import io.eels.Sink
import io.eels.SinkWriter
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.Writer
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector

class OrcSink(val path: Path) : Sink, Logging {
  override fun writer(schema: Schema): SinkWriter = OrcSinkWriter(schema, path)
}

class OrcSinkWriter(schema: Schema, path: Path) : SinkWriter, Logging {
  init {
    logger.debug("Creating orc writer $schema")
  }

  val inspector: StandardListObjectInspector = ObjectInspectorFactory.getStandardListObjectInspector(
    PrimitiveObjectInspectorFactory.javaStringObjectInspector
  )

  val writer: Writer = OrcFile.createWriter(path, OrcFile.writerOptions(Configuration()).inspector(inspector)).apply {
    this.addRow(schema.fieldNames().toTypedArray())
  }

  override fun write(row: Row): Unit {
    synchronized(this) {
      writer.addRow(row.values)
    }
  }

  override fun close(): Unit = writer.close()
}
