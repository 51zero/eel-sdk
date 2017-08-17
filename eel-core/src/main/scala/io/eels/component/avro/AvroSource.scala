package io.eels.component.avro

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels._
import io.eels.datastream.{DataStream, Publisher, Subscriber, Subscription}
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
                              (implicit conf: Configuration, fs: FileSystem)
  extends Publisher[Seq[Row]] with Logging with Using {
  override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
    val deserializer = new AvroDeserializer()
    try {
      using(AvroReaderFns.createAvroReader(path)) { reader =>
        val running = new AtomicBoolean(true)
        subscriber.subscribed(Subscription.fromRunning(running))
        AvroRecordIterator(reader)
          .takeWhile(_ => running.get)
          .map(deserializer.toRow)
          .grouped(DataStream.DefaultBatchSize)
          .foreach(subscriber.next)
        subscriber.completed()
      }
    } catch {
      case t: Throwable => subscriber.error(t)
    }
  }
}

object AvroSource {
  def apply(file: File)(implicit conf: Configuration, fs: FileSystem): AvroSource = AvroSource(new Path(file.getAbsoluteFile.toString))
  def apply(path: java.nio.file.Path)(implicit conf: Configuration, fs: FileSystem): AvroSource = apply(path.toFile)
}