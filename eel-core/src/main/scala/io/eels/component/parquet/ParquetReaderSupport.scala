package io.eels.component.parquet

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroReadSupport}
import org.apache.parquet.hadoop.ParquetReader

object ParquetReaderSupport extends StrictLogging {

  def createReader(path: Path, columns: Seq[String]): ParquetReader[GenericRecord] = {

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

    AvroParquetReader.builder[GenericRecord](path)
      .withConf(configuration)
      .build().asInstanceOf[ParquetReader[GenericRecord]]
  }
}
