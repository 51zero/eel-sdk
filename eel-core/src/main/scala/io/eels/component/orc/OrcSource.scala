package io.eels.component.orc

import com.sksamuel.scalax.io.Using
import io.eels.{FrameSchema, Reader, Row, Source}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.io.orc.{OrcFile, RecordReader}

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
          reader.next(null) match {
            case al: java.util.List[_] => al.asScala.map(_.toString)
            case _ => toString.split(",").toList
          }
        }
      }
    }

    Seq(part)
  }
}
