package io.eels.component.parquet

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.Row
import io.eels.component.avro.AvroRecordFn
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroReadSupport}
import org.apache.parquet.hadoop.ParquetReader

object ParquetIterator extends StrictLogging {

  private def createReader(path: Path, columns: Seq[String]): ParquetReader[GenericRecord] = {

    val config = ConfigFactory.load()
    val parallelism = config.getInt("eel.parquet.parallelism")
    logger.debug(s"Creating parquet reader with parallelism=$parallelism")

    def projection: Schema = {
      val builder = SchemaBuilder.record("dummy").namespace("com")
      columns.foldLeft(builder.fields)((fields, name) => fields.optionalString(name)).endRecord()
    }

    def configuration: Configuration = {
      val conf = new Configuration
      if (columns.nonEmpty) {
        AvroReadSupport.setAvroReadSchema(conf, projection)
        AvroReadSupport.setRequestedProjection(conf, projection)
        conf.set(org.apache.parquet.hadoop.ParquetFileReader.PARQUET_READ_PARALLELISM, parallelism.toString)
      }
      conf
    }

    val conf = configuration
    AvroParquetReader.builder[GenericRecord](path).withConf(conf).build().asInstanceOf[ParquetReader[GenericRecord]]
  }

  def apply(path: Path, columns: Seq[String]): Iterator[Row] = {

    lazy val reader = createReader(path, columns)
    lazy val iter = Iterator.continually(reader.read).takeWhile(_ != null).map { record =>
      if (columns.isEmpty) AvroRecordFn.fromRecord(record) else AvroRecordFn.fromRecord(record, columns)
    }

    new Iterator[Row] {
      override def hasNext: Boolean = {
        val hasNext = iter.hasNext
        if (!hasNext) {
          logger.debug("Closing parquet iterator")
          reader.close()
        }
        hasNext
      }
      override def next(): Row = iter.next()
    }
  }
}
