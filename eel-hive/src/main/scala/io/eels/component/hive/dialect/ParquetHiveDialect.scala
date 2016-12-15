package io.eels.component.hive.dialect

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.component.avro.{AvroSchemaFns, RecordSerializer}
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.component.parquet._
import io.eels.component.parquet.avro.AvroParquetRowWriter
import io.eels.component.parquet.vanilla.ParquetReaderFn
import io.eels.schema.StructType
import io.eels.{CloseableIterator, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}

object ParquetHiveDialect extends HiveDialect with Logging {

  private val config = ConfigFactory.load()
  private val bufferSize = config.getInt("eel.hive.dialect.reader.buffer-size")

  override def read(path: Path,
                    metastoreSchema: StructType,
                    projectionSchema: StructType,
                    predicate: Option[Predicate])
                   (implicit fs: FileSystem, conf: Configuration): CloseableIterator[Seq[Row]] =
    new CloseableIterator[Seq[Row]] {

      // an avro conversion for the projection schema
      val parquetProjectionSchema = ParquetSchemaFns.toParquetSchema(projectionSchema)
      val reader = ParquetReaderFn(path, predicate, Option(parquetProjectionSchema))
      val deser = new ParquetDeserializer()

      override def close(): Unit = {
        super.close()
        reader.close()
      }

      override val iterator: Iterator[Seq[Row]] =
        ParquetIterator(reader).grouped(bufferSize).withPartial(true).map { rows => rows.map(deser.toRow) }
    }

  override def writer(schema: StructType,
                      path: Path,
                      permission: Option[FsPermission])
                     (implicit fs: FileSystem, conf: Configuration): HiveWriter = new HiveWriter {
    ParquetLogMute()

    // hive is case insensitive so we must lower case the fields to keep it consistent
    val avroSchema = AvroSchemaFns.toAvroSchema(schema, caseSensitive = false)
    val writer = new AvroParquetRowWriter(path, avroSchema)
    val serializer = new RecordSerializer(avroSchema)

    override def write(row: Row) {
      require(row.values.nonEmpty, "Attempting to write an empty row")
      val record = serializer.serialize(row)
      writer.write(record)
    }

    override def close() = {
      writer.close()
      permission.foreach(fs.setPermission(path, _))
    }
  }
}