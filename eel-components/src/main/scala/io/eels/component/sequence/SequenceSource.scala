package io.eels.component.sequence

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels._
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable}

case class SequenceSource(path: Path)(implicit conf: Configuration) extends Source with Using with Logging {
  logger.debug(s"Creating sequence source from $path")

  override def schema(): StructType = SequenceSupport.schema(path)
  override def parts(): List[Part] = List(new SequencePart(path))
}

class SequencePart(val path: Path)(implicit conf: Configuration) extends Part with Logging {

  override def iterator(): CloseableIterator[List[Row]] = new CloseableIterator[List[Row]] {

    val reader = SequenceSupport.createReader(path)
    val k = new IntWritable()
    val v = new BytesWritable()
    val schema = SequenceSupport.schema(path)
    var closed = false

    // throw away top row as that's header
    reader.next(k, v)

    override def next(): List[Row] = {
      val row = Row(schema, SequenceSupport.toValues(v).toVector)
      List(row)
    }

    override def hasNext(): Boolean = !closed && reader.next(k, v)

    override def close(): Unit = {
      closed = true
      reader.close()
    }
  }
}