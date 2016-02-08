package io.eels.component.avro

import java.nio.file.Path

import com.sksamuel.scalax.io.Using
import io.eels.{Row, Part, FrameSchema, Source}
import org.apache.avro.file.{SeekableFileInput, DataFileReader}
import org.apache.avro.generic.{GenericRecord, GenericDatumReader}
import scala.collection.JavaConverters._

case class AvroSource(path: Path) extends Source with Using {

  def createReader: DataFileReader[GenericRecord] = {
    val datumReader = new GenericDatumReader[GenericRecord]()
    new DataFileReader[GenericRecord](new SeekableFileInput(path.toFile), datumReader)
  }

  override def schema: FrameSchema = {
    using(createReader) { reader =>
      val record = reader.next()
      val columns = record.getSchema.getFields.asScala.map(_.name)
      FrameSchema(columns)
    }
  }

  override def parts: Seq[Part] = {

    val part = new Part {

      override def iterator: Iterator[Row] = new Iterator[Row] {

        val reader = createReader

        override def hasNext: Boolean = {
          val hasNext = reader.hasNext
          if (!hasNext)
            reader.close()
          hasNext
        }

        override def next: Row = {
          val record = reader.next
          val map = record.getSchema.getFields.asScala.map { field =>
            field.name -> record.get(field.name).toString
          }.toMap
          Row(map)
        }
      }
    }
    Seq(part)
  }
}
