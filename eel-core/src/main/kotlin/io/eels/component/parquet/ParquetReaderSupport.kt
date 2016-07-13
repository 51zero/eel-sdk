package io.eels.component.parquet

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.eels.component.Predicate
import io.eels.component.avro.schemaToAvroSchema
import io.eels.util.Logging
import io.eels.util.Option
import io.eels.util.getOrElse
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader

object ParquetReaderSupport : Logging {

  val config: Config = ConfigFactory.load()

  val parallelism = config.getInt("eel.parquet.parallelism").toString().apply {
    logger.debug("Parquet readers will use parallelism = $this")
  }

  /**
   * Creates a new reader for the given path.
   *
   * @predicate if set then a parquet predicate is applied to the rows
   * @projectionSchema if set then the schema is used to narrow the fields returned
   */
  fun create(path: Path,
             predicate: Option<Predicate>,
             projectionSchema: Option<io.eels.schema.Schema>): ParquetReader<GenericRecord> {

    // The parquet reader can use a projection by setting a projected schema onto a conf object
    fun configuration(): Configuration {
      val conf = Configuration()
      projectionSchema.forEach {
        val projection = schemaToAvroSchema(it, false)
        AvroReadSupport.setAvroReadSchema(conf, projection)
        AvroReadSupport.setRequestedProjection(conf, projection)
        conf.set(org.apache.parquet.hadoop.ParquetFileReader.PARQUET_READ_PARALLELISM, parallelism)
      }
      return conf
    }

    // a filter is set when we have a predicate for the read
    fun filter(): FilterCompat.Filter = predicate.map { FilterCompat.get(it.apply()) }.getOrElse { FilterCompat.NOOP }

    @Suppress("UNCHECKED_CAST")
    return AvroParquetReader.builder<GenericRecord>(path)
        .withConf(configuration())
        .withFilter(filter())
        .build() as ParquetReader<GenericRecord>
  }
}