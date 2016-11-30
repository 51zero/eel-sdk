package io.eels.component.parquet

import com.sksamuel.exts.Logging
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroReadSupport}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.{ParquetOutputFormat, ParquetReader}

/**
  * Helper function to create a parquet reader, using the apache parquet library.
  * The reader supports optional predicate (for row filtering) and a
  * projection schema (for column filtering).
  */
object ParquetReaderFn extends Logging {

  val config: Config = ConfigFactory.load()

  val parallelism = config.getInt("eel.parquet.parallelism").toString()
  logger.debug(s"Parquet readers will use parallelism = $parallelism")

  /**
    * Creates a new reader for the given path.
    *
    * @param predicate        if set then a parquet predicate is applied to the rows
    * @param projectionSchema if set then the schema is used to narrow the fields returned
    */
  def apply(path: Path,
            predicate: Option[Predicate],
            projectionSchema: Option[Schema]): ParquetReader[GenericRecord] = {

    // The parquet reader can use a projection by setting a projected schema onto a conf object
    def configuration(): Configuration = {
      val conf = new Configuration()
      projectionSchema.foreach { it =>
        AvroReadSupport.setAvroReadSchema(conf, it)
        AvroReadSupport.setRequestedProjection(conf, it)
      }
      conf.set(ParquetOutputFormat.ENABLE_JOB_SUMMARY, "false")
      conf.set(org.apache.parquet.hadoop.ParquetFileReader.PARQUET_READ_PARALLELISM, parallelism)
      conf
    }

    // a filter is set when we have a predicate for the read
    def filter(): FilterCompat.Filter = predicate.map(_.parquet).map(FilterCompat.get).getOrElse(FilterCompat.NOOP)

    AvroParquetReader.builder[GenericRecord](path)
      .withConf(configuration())
      .withFilter(filter())
      .build()
      .asInstanceOf[ParquetReader[GenericRecord]]
  }
}