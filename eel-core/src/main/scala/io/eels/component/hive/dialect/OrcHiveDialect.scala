package io.eels.component.hive.dialect

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.component.orc.{OrcStructInspector, StandardStructInspector}
import io.eels.{Column, Field, FrameSchema, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.io.orc.{OrcFile, OrcStruct}

import scala.collection.JavaConverters._

object OrcHiveDialect extends HiveDialect with StrictLogging {

  override def iterator(path: Path, schema: FrameSchema)
                       (implicit fs: FileSystem): Iterator[Row] = {
    logger.debug(s"Creating orc iterator for $path")

    val inspector = OrcStructInspector(schema)
    val reader = OrcFile.createReader(fs, path).rows()

    new Iterator[Row] {
      override def hasNext: Boolean = reader.hasNext
      override def next(): Row = {

        val fields = reader.next(null) match {

          case al: java.util.List[_] =>
            al.asScala.map(_.toString)

          case struct: OrcStruct =>
            inspector.getStructFieldsDataAsList(struct).asScala.map(_.toString).toSeq

          case other =>
            logger.warn(s"Uknown fields type ${other.getClass}; defaulting to splitting on ','")
            toString.split(",").toSeq
        }

        Row(fields.map(Column.apply).toList, fields.map(Field.apply).toList)
      }
    }
  }

  override def writer(schema: FrameSchema, path: Path)
                     (implicit fs: FileSystem): HiveWriter = {
    logger.debug(s"Creating orc writer for $path")

    val inspector = StandardStructInspector(schema)
    val writer = OrcFile.createWriter(path, OrcFile.writerOptions(new Configuration).inspector(inspector))

    new HiveWriter {
      override def close(): Unit = writer.close()
      override def write(row: Row): Unit = {
        writer.addRow(row.fields.map(_.value).toArray)
      }
    }
  }
}
