package io.eels.component.parquet

import com.typesafe.config.ConfigFactory
import io.eels.schema.ColumnType
import io.eels.util.Logging
import io.eels.component.Predicate
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader

object ParquetReaderSupport : Logging {

  val config = ConfigFactory.load()
  val parallelism: String = config.getInt("eel.parquet.parallelism").toString()

  init {
    logger.debug("Parquet readers will have parallelism = $parallelism")
  }

  /**
   * Creates a new reader from the given path. If projection is set then a projected
   * schema is generated from the given schema.
   */
  fun create(path: Path,
             isProjection: Boolean,
             predicate: Predicate?,
             schema: io.eels.schema.Schema?): ParquetReader<GenericRecord> {
    require(!isProjection || schema != null, { "Schema cannot be null if projection is set" })

    fun projection(): Schema {
      val builder = SchemaBuilder.record("row").namespace("namespace")
      return schema!!.columns.fold(builder.fields(), { fields, col ->
        val name = col.name
        when (col.type) {
          ColumnType.BigInt -> fields.optionalLong(name)
          ColumnType.Boolean -> fields.optionalBoolean(name)
          ColumnType.Double -> fields.optionalDouble(name)
          ColumnType.Float -> fields.optionalFloat(name)
          ColumnType.Int -> fields.optionalInt(name)
          ColumnType.Long -> fields.optionalLong(name)
          ColumnType.String -> fields.optionalString(name)
          ColumnType.Short -> fields.optionalInt(name)
          else -> {
            logger.warn("Unknown schema type ${col.type}; defaulting to string")
            fields.optionalString(name)
          }
        }
      }).endRecord()
    }

    fun configuration(): Configuration {
      val conf = Configuration()
      if (isProjection) {


        AvroReadSupport.setAvroReadSchema(conf, projection())
        AvroReadSupport.setRequestedProjection(conf, projection())
        conf.set(org.apache.parquet.hadoop.ParquetFileReader.PARQUET_READ_PARALLELISM, parallelism)
      }
      return conf
    }

    fun filter(): FilterCompat.Filter = when (predicate) {
      null -> FilterCompat.NOOP
      else -> FilterCompat.get(predicate.apply())
    }

    @Suppress("UNCHECKED_CAST")
    return AvroParquetReader.builder<GenericRecord>(path)
        .withConf(configuration())
        .withFilter(filter())
        .build() as ParquetReader<GenericRecord>
  }
}