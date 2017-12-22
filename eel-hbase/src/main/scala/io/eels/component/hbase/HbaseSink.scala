package io.eels.component.hbase

import java.nio
import java.util.concurrent.atomic.AtomicInteger

import com.sksamuel.exts.Logging
import com.typesafe.config.{Config, ConfigFactory}
import io.eels.schema.{Field, StringType, StructType}
import io.eels.{Sink, SinkWriter}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.security.UserGroupInformation

object HbaseSink {
  private val unverifiedField = Field("eel_dummy", StringType)
  private val unverifiedSchema = StructType(Vector(unverifiedField))
  private val bufferSizeProp = "eel.hbase.bufferSize"
  val config: Config = ConfigFactory.load()
  val bufferSize: Int = if (config.hasPath(bufferSizeProp)) config.getInt(bufferSizeProp) else 10000
}

case class HbaseSink(namespace: String,
                     table: String,
                     hbaseSchema: StructType = HbaseSink.unverifiedSchema,
                     principal: Option[String] = None,
                     keytabPath: Option[nio.file.Path] = None,
                     bufferSize: Int = HbaseSink.bufferSize,
                     maxKeyValueSize: Option[Int] = None,
                     writeBufferSize: Option[Long] = None,
                     writeRowBatchSize: Int = 100,
                     schemaCaseSensitive: Boolean = true,
                     connection: Connection = ConnectionFactory.createConnection(HBaseConfiguration.create()),
                     serializer: HbaseSerializer = HbaseSerializer.standardSerializer) extends Sink with AutoCloseable with Logging {

  private val caseEquals = if (!schemaCaseSensitive) (value1: String, value2: String) => value1.toLowerCase == value2.toLowerCase
  else (value1: String, value2: String) => value1 == value2

  override def open(schema: StructType, numberOfWriters: Int): Seq[SinkWriter] = makeHbaseSinkWriter(schema, numberOfWriters)

  override def open(schema: StructType): SinkWriter = makeHbaseSinkWriter(schema, 1).head

  override def close(): Unit = connection.close()

  def withBufferSize(bufferSize: Int): HbaseSink = copy(bufferSize = bufferSize)

  def withRowBatchSize(writeRowBatchSize: Int): HbaseSink = copy(writeRowBatchSize = writeRowBatchSize)

  def withMaxKeyValueSize(maxKeyValueSize: Int): HbaseSink = copy(maxKeyValueSize = Option(maxKeyValueSize))

  def withWriteBufferSize(writeBufferSize: Long): HbaseSink = copy(writeBufferSize = Option(writeBufferSize))

  def withSerializer(serializer: HbaseSerializer): HbaseSink = copy(serializer = serializer)

  def withFieldKey(name: String): HbaseSink = withFieldKey(Field(name = name))

  def withFieldKey(field: Field): HbaseSink = copy(
    hbaseSchema = hbaseSchema.addField(Field(field.name, field.dataType, field.nullable, field.partition, field.comment, key = true, field.defaultValue, field.metadata))
  )

  def withColumnFamily(cf: String, fields: String*): HbaseSink = withColumnFamily(cf, fields.toList)

  def withColumnFamily(cf: String, fields: List[String]): HbaseSink = {
    copy(
      hbaseSchema = fields.foldLeft(hbaseSchema) { (curSchema, name) =>
        curSchema.addFieldIfNotExists(name)
          .replaceField(name, curSchema.fields.find(f => caseEquals(f.name, name)).get.withColumnFamily(cf))
      }
    )
  }

  def withFieldsFromHiveSchema(hiveDatabase: String, hiveTable: String): HbaseSink = {
    copy(hbaseSchema = StructType(HbaseHiveOps.fromHiveHbaseSchema(hiveDatabase, hiveTable)))
  }

  def withSchema(schema: StructType): HbaseSink = copy(hbaseSchema = schema)

  def withKeytabFile(principal: String, keytabPath: nio.file.Path): HbaseSink = {
    UserGroupInformation.loginUserFromKeytab(principal, keytabPath.toString)
    copy(principal = Option(principal), keytabPath = Option(keytabPath))
  }

  def withSchemaCaseSensitive(schemaCaseSensitive: Boolean): HbaseSink = copy(schemaCaseSensitive = schemaCaseSensitive)

  def withConfiguration(configuration: HBaseConfiguration): HbaseSink = copy(connection = ConnectionFactory.createConnection(configuration))

  private def makeHbaseSinkWriter(schema: StructType, numberOfWriters: Int): Seq[SinkWriter] = {
    val mappedSchema = mapSchemaToHbase(schema).removeField(HbaseSink.unverifiedField.name)
    checkRowKeyMappings(mappedSchema)
    checkColumnFamilyMappings(mappedSchema)
    HbaseSinkWriter(
      namespace = namespace,
      table = table,
      numberOfWriters = new AtomicInteger(numberOfWriters),
      schema = mappedSchema,
      maxKeyValueSize = maxKeyValueSize,
      writeBufferSize = writeBufferSize,
      writeRowBatchSize = writeRowBatchSize,
      serializer = serializer,
      connection = connection)
  }

  // Map incoming schema to keys and column families
  private def mapSchemaToHbase(schema: StructType): StructType = {
    schema.fields.foldLeft(schema) { (curSchema, field) =>
      val mappedField = hbaseSchema.fields.find(f => caseEquals(f.name, field.name)).getOrElse(sys.error(s"No mapping found for source field '${field.name}'"))
      curSchema.replaceField(field.name, field.copy(key = mappedField.key, columnFamily = mappedField.columnFamily))
    }
  }

  private def checkColumnFamilyMappings(mappedSchema: StructType): Unit = {
    val noColumnFamiliyFields = mappedSchema.fields.filter(f => !f.key && f.columnFamily.isEmpty)
    if (noColumnFamiliyFields.nonEmpty) sys.error(s"Missing column family mappings for fields: ${noColumnFamiliyFields.map(_.name).mkString(",")}")
  }

  private def checkRowKeyMappings(mappedSchema: StructType): Unit = {
    val keyFields = mappedSchema.fields.filter(_.key)
    if (keyFields.isEmpty) sys.error("Missing key mapping")
    else if (keyFields.length > 1) sys.error(s"Only 1 key mapping allowed out of fields: ${keyFields.map(_.name).mkString(",")}")
  }
}
