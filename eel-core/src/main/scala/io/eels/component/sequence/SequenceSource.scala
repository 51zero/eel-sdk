package io.eels.component.sequence

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels._
import io.eels.component.FlowableIterator
import io.eels.schema.StructType
import io.reactivex.Flowable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable}

case class SequenceSource(path: Path)(implicit conf: Configuration) extends Source with Using with Logging {
  logger.debug(s"Creating sequence source from $path")

  override def schema: StructType = SequenceSupport.schema(path)
  override def parts(): List[Part] = List(new SequencePart(path))
}

class SequencePart(val path: Path)(implicit conf: Configuration) extends Part with Logging {

  override def open(): Flowable[Row] = {

    val reader = SequenceSupport.createReader(path)
    val k = new IntWritable()
    val v = new BytesWritable()
    val schema = SequenceSupport.schema(path)

    // throw away the header
    reader.next(k, v)

    val iterator: Iterator[Row] = new Iterator[Row] {
      override def next(): Row = Row(schema, SequenceSupport.toValues(v).toVector)
      override def hasNext(): Boolean = reader.next(k, v)
    }

    FlowableIterator(iterator, reader.close _)
  }
}