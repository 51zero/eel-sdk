package io.eels.component.parquet

import java.util

import io.eels.{Row, RowBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.io.api._
import org.apache.parquet.schema.MessageType

class RowReadSupport extends ReadSupport[Row] {

  override def prepareForRead(configuration: Configuration,
                              keyValueMetaData: util.Map[String, String],
                              fileSchema: MessageType,
                              readContext: ReadContext): RecordMaterializer[Row] = {
    new RowMaterializer(fileSchema, readContext)
  }

  override def init(configuration: Configuration,
                    keyValueMetaData: util.Map[String, String],
                    fileSchema: MessageType): ReadSupport.ReadContext = {
    val partialSchemaString = configuration.get(ReadSupport.PARQUET_READ_SCHEMA)
    val requestedProjection = ReadSupport.getSchemaForRead(fileSchema, partialSchemaString)
    new ReadSupport.ReadContext(requestedProjection)
  }
}

class RowMaterializer(fileSchema: MessageType,
                      readContext: ReadContext) extends RecordMaterializer[Row] {

  val schema = ParquetSchemaFns.fromParquetGroupType(fileSchema)
  val builder = new RowBuilder(schema)

  override def getRootConverter: GroupConverter = new GroupConverter {
    override def getConverter(fieldIndex: Int): Converter = new DefaultPrimitiveConverter(builder)
    override def end(): Unit = ()
    override def start(): Unit = builder.reset()
  }

  override def getCurrentRecord: Row = builder.build()
}

class DefaultPrimitiveConverter(builder: RowBuilder) extends PrimitiveConverter {
  override def isPrimitive: Boolean = true
  override def addBinary(value: Binary): Unit = builder.add(value.getBytes)
  override def addDouble(value: Double): Unit = builder.add(value)
  override def addLong(value: Long): Unit = builder.add(value)
}