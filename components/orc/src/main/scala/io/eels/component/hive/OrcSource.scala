package io.eels.component.hive

import io.eels.{Column, Field, Reader, Row, Source}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.io.orc.OrcFile

import scala.collection.JavaConverters._

case class OrcSource(path: Path)(implicit fs: FileSystem) extends Source {

  override def reader: Reader = new Reader {

    val reader = OrcFile.createReader(fs, path).rows()

    override def close(): Unit = reader.close()
    override def iterator: Iterator[Row] = new Iterator[Row] {
      override def hasNext: Boolean = reader.hasNext
      override def next(): Row = {
        val fields = reader.next(null) match {
          case al: java.util.List[_] => al.asScala.map(_.toString)
          case _ => toString.split(",").toList
        }
        Row(fields.map(Column.apply), fields.map(Field.apply))
      }
    }
  }
}
