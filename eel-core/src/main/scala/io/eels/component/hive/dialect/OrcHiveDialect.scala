package io.eels.component.hive.dialect

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.component.orc.{OrcStructInspector, StandardStructInspector}
import io.eels.{InternalRow, Schema, SourceReader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.io.orc.{OrcFile, OrcStruct}

import scala.collection.JavaConverters._

object OrcHiveDialect extends HiveDialect with StrictLogging {

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

  // todo implement column pushdown
  override def reader(path: Path, tableSchema: Schema, requestedSchema: Schema)
                     (implicit fs: FileSystem): SourceReader = {
    logger.debug(s"Creating orc iterator for $path")

    new SourceReader {

      val inspector = OrcStructInspector(tableSchema)
      val reader = OrcFile.createReader(fs, path).rows()

      override def close(): Unit = reader.close()

      override def iterator: Iterator[InternalRow] = new Iterator[InternalRow] {

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
  }
}
