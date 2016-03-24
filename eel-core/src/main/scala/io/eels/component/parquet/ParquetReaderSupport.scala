package io.eels.component.parquet

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.SchemaType
import io.eels.component.hive.Predicate
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroReadSupport}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.compat.FilterCompat.{Filter, NoOpFilter}
import org.apache.parquet.hadoop.ParquetReader

object ParquetReaderSupport extends StrictLogging {

  val config = ConfigFactory.load()

  lazy val parallelism = {
    val parallelism = config.getInt("eel.parquet.parallelism")
    logger.debug(s"Creating parquet reader with parallelism = $parallelism")
    parallelism.toString
  }

  /**
    * Creates a new reader from the given path. If projection is set then a projected schema is generated
    * from the given schema.
    */
  def createReader(path: Path,
                   isProjection: Boolean,
                   predicate: Option[Predicate],
                   schema: io.eels.Schema): ParquetReader[GenericRecord] = {
    require(!isProjection || schema != null, "Schema cannot be null if projection is set")

    def projection: Schema = {
      val builder = SchemaBuilder.record("row").namespace("namespace")
      schema.columns.foldLeft(builder.fields) { (fields, col) =>
        val schemaType = col.`type`
        val name = col.name
        schemaType match {
          case SchemaType.BigInt => fields.optionalLong(name)
          case SchemaType.Boolean => fields.optionalBoolean(name)
          case SchemaType.Double => fields.optionalDouble(name)
          case SchemaType.Float => fields.optionalFloat(name)
          case SchemaType.Int => fields.optionalInt(name)
          case SchemaType.Long => fields.optionalLong(name)
          case SchemaType.String => fields.optionalString(name)
          case SchemaType.Short => fields.optionalInt(name)
          case _ =>
            logger.warn(s"Unknown schema type $schemaType; default to string")
            fields.optionalString(name)
        }
      }.endRecord()
    }

    def configuration: Configuration = {
      val conf = new Configuration
      if (isProjection) {
        AvroReadSupport.setAvroReadSchema(conf, projection)
        AvroReadSupport.setRequestedProjection(conf, projection)
        conf.set(org.apache.parquet.hadoop.ParquetFileReader.PARQUET_READ_PARALLELISM, parallelism)
      }
      conf
    }

    AvroParquetReader.builder[GenericRecord](path)
      .withConf(configuration)
      .withFilter(predicate.fold(FilterCompat.NOOP)(p => FilterCompat.get(p())))
      .build().asInstanceOf[ParquetReader[GenericRecord]]
  }
}
