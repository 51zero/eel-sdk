package io.eels.component.hive.dialect

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.component.orc.{OrcStructInspector, StandardStructInspector}
import io.eels.{Schema, InternalRow}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.io.orc.{OrcFile, OrcStruct}

import scala.collection.JavaConverters._

object OrcHiveDialect extends HiveDialect with StrictLogging {

  // todo implement column pushdown
  override def iterator(path: Path, schema: Schema, ignored: Seq[String])
                       (implicit fs: FileSystem): Iterator[InternalRow] = {
    logger.debug(s"Creating orc iterator for $path")

    val inspector = OrcStructInspector(schema)
    val reader = OrcFile.createReader(fs, path).rows()

    new Iterator[InternalRow] {
      override def hasNext: Boolean = reader.hasNext
      override def next(): InternalRow = {
        reader.next(null) match {

          case al: java.util.List[_] =>
            al.asScala.map(_.toString)

          case struct: OrcStruct =>
            inspector.getStructFieldsDataAsList(struct).asScala.map(_.toString)

          case other =>
            logger.warn(s"Uknown fields type ${other.getClass}; defaulting to splitting on ','")
            toString.split(",").toSeq
        }
      }
    }
  }

  override def writer(schema: Schema, path: Path)
                     (implicit fs: FileSystem): HiveWriter = {
    logger.debug(s"Creating orc writer for $path")

    val inspector = StandardStructInspector(schema)
    val writer = OrcFile.createWriter(path, OrcFile.writerOptions(new Configuration).inspector(inspector))

    new HiveWriter {
      override def close(): Unit = writer.close()
      override def write(row: InternalRow): Unit = {
        // builds a map of the column names to the row values (by using the source schema), then generates
        // a new sequence of values ordered by the columns in the target schema
        val map = schema.columnNames.zip(row).map { case (columName, value) => columName -> value }.toMap
        writer.addRow(row.toArray)
      }
    }
  }
}
