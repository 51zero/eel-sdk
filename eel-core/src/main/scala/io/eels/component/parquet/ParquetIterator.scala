package io.eels.component.parquet

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.Row
import io.eels.component.avro.AvroRecordFn
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroReadSupport, AvroParquetReader}
import org.apache.parquet.hadoop.ParquetReader
import scala.collection.JavaConverters._

object ParquetIterator extends StrictLogging {

  private def createReader(path: Path, columns: Seq[String]): ParquetReader[GenericRecord] = {

    def projection: Schema = {
      val fields = columns.map(name => new Schema.Field(name, Schema.create(Schema.Type.STRING), null, null))
      val schema = Schema.createRecord("dummy", null, "com", false)
      schema.setFields(fields.asJava)
      schema
    }

    def configuration: Configuration = {
      val conf = new Configuration
      AvroReadSupport.setRequestedProjection(conf, projection)
      conf
    }

    val conf = configuration
    AvroParquetReader.builder[GenericRecord](path).withConf(conf).build().asInstanceOf[ParquetReader[GenericRecord]]
  }

  def apply(path: Path, columns: Seq[String]): Iterator[Row] = {

    lazy val reader = createReader(path, columns)
    lazy val iter = Iterator.continually(reader.read).takeWhile(_ != null).map(AvroRecordFn.fromRecord(_, columns))

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
