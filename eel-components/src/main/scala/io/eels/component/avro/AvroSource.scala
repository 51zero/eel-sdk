package io.eels.component.avro

import java.io.File

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels._
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

  override def parts(): List[Part] = List(AvroSourcePart(path))
}

case class AvroSourcePart(path: Path)
                         (implicit conf: Configuration, fs: FileSystem) extends Part with Logging {

  override def iterator(): CloseableIterator[Seq[Row]] = new CloseableIterator[Seq[Row]] {

    val deserializer = new AvroDeserializer()
    val reader = AvroReaderFns.createAvroReader(path)

    override def close(): Unit = {
      super.close()
      reader.close()
    }

    override val iterator: Iterator[Seq[Row]] = AvroRecordIterator(reader).map { record =>
      deserializer.toRow(record)
    }.grouped(1000).withPartial(true)
  }
}

object AvroSource {
  def apply(file: File)(implicit conf: Configuration, fs: FileSystem): AvroSource = AvroSource(new Path(file.getAbsoluteFile.toString))
  def apply(path: java.nio.file.Path)(implicit conf: Configuration, fs: FileSystem): AvroSource = apply(path.toFile)
}