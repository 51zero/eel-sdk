package io.eels.component.parquet.avro

import io.eels.Predicate
import io.eels.component.parquet.{ParquetPredicateBuilder, ParquetReaderConfig}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroReadSupport}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader

/**
  * Helper function to create a parquet reader, using the apache parquet library.
  * The reader supports optional predicate (for row filtering) and a
  * projection schema (for column filtering).
  */
object AvroParquetReaderFn {

  private val config = ParquetReaderConfig()

  /**
    * Creates a new reader for the given path.
    *
    * @param predicate        if set then a parquet predicate is applied to the rows
    * @param projectionSchema if set then the schema is used to narrow the fields returned
    */
  def apply(path: Path,
            predicate: Option[Predicate],
            projectionSchema: Option[Schema])(implicit conf: Configuration): ParquetReader[GenericRecord] = {

    // The parquet reader can use a projection by setting a projected schema onto a conf object
    def configuration(): Configuration = {
      val newconf = new Configuration(conf)
      projectionSchema.foreach { it =>
        AvroReadSupport.setAvroReadSchema(newconf, it)
        AvroReadSupport.setRequestedProjection(newconf, it)
      }
      //conf.set(ParquetInputFormat.DICTIONARY_FILTERING_ENABLED, "true")
      newconf.set(org.apache.parquet.hadoop.ParquetFileReader.PARQUET_READ_PARALLELISM, config.parallelism.toString)
      newconf
    }

    // a filter is set when we have a predicate for the read
    def filter(): FilterCompat.Filter = predicate.map(ParquetPredicateBuilder.build)
      .map(FilterCompat.get)
      .getOrElse(FilterCompat.NOOP)

    AvroParquetReader.builder[GenericRecord](path)
      .withCompatibility(false)
      .withConf(configuration())
      .withFilter(filter())
      .build()
      .asInstanceOf[ParquetReader[GenericRecord]]
  }
}