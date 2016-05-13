package io.eels.component.orc

import io.eels._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.io.orc.{OrcFile, RecordReader}

case class OrcSource(path: Path)(implicit fs: FileSystem) extends Source with Using {

  def createReader: RecordReader = OrcFile.createReader(fs, path).rows()

  override def schema: Schema = {
    using(createReader) { reader =>
      val fields = reader.next(null) match {
        case al: java.util.List[_] => al.asScala.map(_.toString)
        case _ => toString.split(",").toList
      }
      Schema(fields)
    }
  }
  override def parts: Seq[Part] = {
    val part = new Part {
      override def reader = new SourceReader {

        val reader = OrcFile.createReader(fs, path).rows()
        reader.next(null)

        override def close(): Unit = reader.close()
        override def iterator: Iterator[InternalRow] = new Iterator[InternalRow] {

          override def hasNext: Boolean = reader.hasNext
          override def next(): InternalRow = {
            reader.next(null) match {
              case al: java.util.List[_] => al.asScala.map(_.toString)
              case _ => toString.split(",").toList
            }
          }
        }
      }
    }
    Seq(part)
  }
}
