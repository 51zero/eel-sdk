package io.eels.component.parquet

import com.sksamuel.exts.Logging
import com.typesafe.config.{Config, ConfigFactory}
import io.eels.Predicate
import io.eels.component.avro.AvroSchemaFns
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroReadSupport}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader

object ParquetReaderFns extends Logging {

  val config: Config = ConfigFactory.load()

  val parallelism = config.getInt("eel.parquet.parallelism").toString()
    logger.debug("Parquet readers will use parallelism = $this")

  /**
   * Creates a new reader for the given path.
   *
    * @param predicate        if set then a parquet predicate is applied to the rows
    * @param projectionSchema if set then the schema is used to narrow the fields returned
   */
  def createReader(path: Path,
                   predicate: Option[Predicate],
                   projectionSchema: Option[io.eels.schema.Schema]): ParquetReader[GenericRecord] = {

    // The parquet reader can use a projection by setting a projected schema onto a conf object
    def configuration(): Configuration = {
      val conf = new Configuration()
      projectionSchema.foreach { it =>
        val projection = AvroSchemaFns.toAvroSchema(it, false)
        AvroReadSupport.setAvroReadSchema(conf, projection)
        AvroReadSupport.setRequestedProjection(conf, projection)
        conf.set(org.apache.parquet.hadoop.ParquetFileReader.PARQUET_READ_PARALLELISM, parallelism)
      }
      conf
    }

    // a filter is set when we have a predicate for the read
    def filter(): FilterCompat.Filter = predicate.map { pred => FilterCompat.get(pred()) }.getOrElse(FilterCompat.NOOP)

    AvroParquetReader.builder[GenericRecord](path)
        .withConf(configuration())
        .withFilter(filter())
      .build().asInstanceOf[ParquetReader[GenericRecord]]
  }
}