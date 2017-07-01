package io.eels.component.avro

import java.io.File

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels._
import io.eels.component.FlowableIterator
import io.eels.schema.StructType
import io.reactivex.Flowable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

case class AvroSource(path: Path)
                     (implicit conf: Configuration, fs: FileSystem) extends Source with Using {

  override lazy val schema: StructType = {
    using(AvroReaderFns.createAvroReader(path)) { reader =>
      val record = reader.next()
      AvroSchemaFns.fromAvroSchema(record.getSchema)
    }
  }

  override def parts(): List[Part] = List(AvroSourcePart(path))
}

case class AvroSourcePart(path: Path)
                         (implicit conf: Configuration, fs: FileSystem) extends Part with Logging {
  /**
    * Returns the data contained in this part in the form of an iterator. This function should return a new
    * iterator on each invocation. The iterator can be lazily initialized to the first read if required.
    */
  override def open(): Flowable[Row] = {

    val deserializer = new AvroDeserializer()
    val reader = AvroReaderFns.createAvroReader(path)

    val iterator: Iterator[Row] = AvroRecordIterator(reader).map { record =>
      deserializer.toRow(record)
    }

    FlowableIterator(iterator, reader.close _)
  }
}

object AvroSource {
  def apply(file: File)(implicit conf: Configuration, fs: FileSystem): AvroSource = AvroSource(new Path(file.getAbsoluteFile.toString))
  def apply(path: java.nio.file.Path)(implicit conf: Configuration, fs: FileSystem): AvroSource = apply(path.toFile)
}