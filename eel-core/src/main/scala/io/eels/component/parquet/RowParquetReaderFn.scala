package io.eels.component.parquet

import com.sksamuel.exts.Logging
import io.eels.{Predicate, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetReader}
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.schema.Type

/**
  * Helper function to create a native parquet reader for Row objects, using the apache parquet library.
  * The reader supports optional predicate (for row filtering) and a
  * projection schema (for column filtering).
  */
object RowParquetReaderFn extends Logging {

  private val config = ParquetReaderConfig()

  /**
    * Creates a new reader for the given path.
    *
    * @param predicate        if set then a parquet predicate is applied to the rows
    * @param projectionSchema if set then the schema is used to narrow the fields returned
    */
  def apply(path: Path,
            predicate: Option[Predicate],
            projectionSchema: Option[Type])(implicit conf: Configuration): ParquetReader[Row] = {
    logger.debug(s"Opening parquet reader for $path")

    // The parquet reader can use a projection by setting a projected schema onto the supplied conf object
    def configuration(): Configuration = {
      val newconf = new Configuration(conf)
      projectionSchema.foreach { it =>
        newconf.set(ReadSupport.PARQUET_READ_SCHEMA, it.toString)
      }
      newconf.set(ParquetInputFormat.DICTIONARY_FILTERING_ENABLED, "true")
      newconf.set(org.apache.parquet.hadoop.ParquetFileReader.PARQUET_READ_PARALLELISM, config.parallelism.toString)
      newconf
    }

    // a filter is set when we have a predicate for the read
    def filter(): FilterCompat.Filter = predicate.map(ParquetPredicateBuilder.build)
      .map(FilterCompat.get)
      .getOrElse(FilterCompat.NOOP)

    ParquetReader.builder(new RowReadSupport, path)
      .withConf(configuration())
      .withFilter(filter())
      .build()
  }
}