package io.eels.component.avro

import java.io.File

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels._
import io.eels.datastream.{Publisher, Subscriber}
import io.eels.schema.StructType
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

  override def parts(): Seq[Publisher[Seq[Row]]] = Seq(AvroSourcePublisher(path))
}

case class AvroSourcePublisher(path: Path)
                              (implicit conf: Configuration, fs: FileSystem) extends Publisher[Seq[Row]] with Logging {
  override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
    try {
      val deserializer = new AvroDeserializer()
      val reader = AvroReaderFns.createAvroReader(path)
      AvroRecordIterator(reader).map(deserializer.toRow).grouped(1000).foreach(subscriber.next)
      subscriber.completed()
    } catch {
      case t: Throwable => subscriber.error(t)
    }
  }
}

object AvroSource {
  def apply(file: File)(implicit conf: Configuration, fs: FileSystem): AvroSource = AvroSource(new Path(file.getAbsoluteFile.toString))
  def apply(path: java.nio.file.Path)(implicit conf: Configuration, fs: FileSystem): AvroSource = apply(path.toFile)
}