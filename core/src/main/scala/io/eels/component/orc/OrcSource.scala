package io.eels.component.orc

import com.sksamuel.scalax.io.Using
import io.eels.{Column, Field, FrameSchema, Reader, Row, Source}
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConverters._

case class OrcSource(path: Path)(implicit fs: FileSystem) extends Source with Using {

  def createReader: RecordReader = OrcFile.createReader(fs, path).rows()

  override def schema: FrameSchema = {
    using(createReader) { reader =>
      val fields = reader.next(null) match {
        case al: java.util.List[_] => al.asScala.map(_.toString)
        case _ => toString.split(",").toList
      }
      FrameSchema(fields)
    }
  }
  override def readers: Seq[Reader] = {

    val reader = OrcFile.createReader(fs, path).rows()

    val part = new Reader {
      override def close(): Unit = reader.close()
      override def iterator: Iterator[Row] = new Iterator[Row] {

        override def hasNext: Boolean = reader.hasNext
        override def next(): Row = {
          val fields = reader.next(null) match {
            case al: java.util.List[_] => al.asScala.map(_.toString)
            case _ => toString.split(",").toList
          }
          Row(fields.map(Column.apply).toList, fields.map(Field.apply).toList)
        }
      }
    }

    Seq(part)
  }
}
