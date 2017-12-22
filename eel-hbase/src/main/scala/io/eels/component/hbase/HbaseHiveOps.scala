package io.eels.component.hbase

import io.eels.schema
import io.eels.schema._
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.{HiveMetaStoreClient, IMetaStoreClient}
import org.h2.table.TableType

/**
  * Hive/HBase specific helpers
  */
object HbaseHiveOps {

  val HBASE_HIVE_COLUMN_MAPPING = "hbase.columns.mapping"
  val HBASE_TABLE_NAME_PROP = "hbase.table.name"
  val EEL_HBASE_ORDERED_ASCENDING_SERIALIZATION = "eel.hbase.ordered.ascend.serialization"
  val EEL_HBASE_ORDERED_DESCENDING_SERIALIZATION = "eel.hbase.ordered.descend.serialization"

  private val CharRegex = "char\\((.*?)\\)".r
  private val VarcharRegex = "varchar\\((.*?)\\)".r
  private val DecimalRegex = "decimal\\((\\d+),(\\d+)\\)".r
  private val StructRegex = "struct<(.*?)>".r
  private val ArrayRegex = "array<(.*?)>".r

  def hiveColumnMappings(spec: TableSpec): Vector[(String, String, Boolean)] = spec.serdeInfoParams(HBASE_HIVE_COLUMN_MAPPING).split(",").map { f =>
    val s = f.split(":")
    (s.last.split("#").head, s.head, s.head.isEmpty)
  } toVector

  def fromHiveHbaseSchema(hiveDatabase: String, hiveTable: String): Seq[Field] = {
    implicit val hiveMetastoreClient: IMetaStoreClient = new HiveMetaStoreClient(new HiveConf())
    fromTableSpecFields(spec(hiveDatabase, hiveTable))
  }

  def fromTableSpecFields(tableSpec: TableSpec): Seq[Field] = {
    val mappings = hiveColumnMappings(tableSpec)
    for ((field, index) <- tableSpec.columns.zipWithIndex) yield {
      val (column, columnFamily, key) = mappings(index)
      fromHiveField(columnFamily, column, key, field)
    }
  }

  def hbaseInfoFromHive(hiveDatabase: String, hiveTable: String): HbaseHiveInfo = {
    implicit val hiveMetastoreClient: IMetaStoreClient = new HiveMetaStoreClient(new HiveConf())
    val tableSpec = spec(hiveDatabase, hiveTable)

    // Give preference to TBLPROPERTIES over SERDEPROPERTIES - do both for backwards compatibility
    val tablePropValue = tableProperty(HBASE_TABLE_NAME_PROP, tableSpec.params, tableSpec, throwIfNotExists = false)
    val split = (if (tablePropValue.nonEmpty) tablePropValue else tableProperty(HBASE_TABLE_NAME_PROP, tableSpec.serdeInfoParams, tableSpec)).split(":")
    if (split.isEmpty) sys.error(s"'$HBASE_TABLE_NAME_PROP' table property is empty in '$hiveDatabase.$hiveTable'")
    val (hbaseNameSpace, hbaseTable) = if (split.length == 1) ("default", split.head) else (split.head, split(1))

    HbaseHiveInfo(
      hbaseNameSpace,
      hbaseTable,
      schema.StructType(fromTableSpecFields(tableSpec)),
      if (tableProperty(EEL_HBASE_ORDERED_ASCENDING_SERIALIZATION, tableSpec.params, tableSpec, throwIfNotExists = false).nonEmpty) {
        HbaseSerializer.standardSerializer
      } else if (tableProperty(EEL_HBASE_ORDERED_DESCENDING_SERIALIZATION, tableSpec.params, tableSpec, throwIfNotExists = false).nonEmpty) {
        HbaseSerializer.orderedAscendingSerializer
      } else {
        HbaseSerializer.orderedDescendingSerializer
      }
    )
  }

  def tableProperty(property: String, params: Map[String, String], tableSpec: TableSpec, throwIfNotExists: Boolean = true): String = {
    val value = params.get(property)
    if (throwIfNotExists && (value.isEmpty || value.get.isEmpty)) sys.error(s"'$property' table property doesn't exist in '${tableSpec.databaseName}.${tableSpec.tableName}'")
    value.getOrElse("")
  }

  def serdeInfoProperty(property: String, params: Map[String, String], tableSpec: TableSpec, throwIfNotExists: Boolean = true): String = {
    val value = tableSpec.params.get(property)
    if (throwIfNotExists && (value.isEmpty || value.get.isEmpty)) sys.error(s"'$property' table property doesn't exist in '${tableSpec.databaseName}.${tableSpec.tableName}'")
    value.getOrElse("")
  }

  def fromHiveField(columnFamily: String, column: String, key: Boolean, fieldSchema: FieldSchema): Field = {
    fromHive(columnFamily, column, key, fieldSchema.getName, fieldSchema.getType, fieldSchema.getComment)
  }

  def fromHive(columnFamily: String, column: String, key: Boolean, name: String, typeInfo: String, comment: String): Field = {
    Field(name = column,
      dataType = fromHiveType(typeInfo),
      comment = Option(comment),
      key = key,
      metadata = Map.empty,
      columnFamily = Option(columnFamily)
    )
  }

  // Converts a Hive type string into an EEL DataType
  def fromHiveType(descriptor: String): DataType = descriptor match {
    case ArrayRegex(element) => ArrayType(fromHiveType(element))
    case "bigint" => BigIntType
    case "binary" => BinaryType
    case "boolean" => BooleanType
    case CharRegex(size) => CharType(size.toInt)
    case DecimalRegex(precision, scale) => DecimalType(precision.toInt, scale.toInt)
    case "date" => DateType
    case "double" => DoubleType
    case "float" => FloatType
    case "int" => IntType.Signed
    case "smallint" => ShortType.Signed
    case "string" => StringType
    case "timestamp" => TimestampMillisType
    case "tinyint" => ByteType.Signed
    case StructRegex(struct) => StructType(
      struct.split(",").map(_.split(":")).collect {
        case Array(fieldName, typeInfo) => Field(fieldName, fromHiveType(typeInfo))
      })
    case VarcharRegex(size) => VarcharType(size.toInt)
    case _ => sys.error(s"Unsupported Hive Type [$descriptor]")
  }

  def spec(dbName: String, tableName: String)(implicit client: IMetaStoreClient): TableSpec = {
    import scala.collection.JavaConverters._
    val table = client.getTable(dbName, tableName)
    val tableType = TableType.values().find(_.name.toLowerCase == table.getTableType.toLowerCase)
      .getOrElse(sys.error("Hive table type is not supported by this version of Hive"))
    TableSpec(
      tableName,
      dbName,
      tableType,
      table.getSd.getCols.asScala.toVector,
      table.getSd.getLocation,
      table.getSd.getNumBuckets,
      table.getSd.getBucketCols.asScala.toList,
      table.getSd.getSerdeInfo.getParameters.asScala.toMap,
      table.getSd.getParameters.asScala.toMap,
      table.getParameters.asScala.toMap,
      table.getSd.getInputFormat,
      table.getSd.getOutputFormat,
      table.getSd.getSerdeInfo.getName,
      table.getRetention,
      table.getCreateTime,
      table.getLastAccessTime,
      table.getOwner)
  }

  case class TableSpec(tableName: String,
                       databaseName: String,
                       tableType: TableType,
                       columns: Vector[FieldSchema],
                       location: String,
                       numBuckets: Int,
                       bucketNames: List[String],
                       serdeInfoParams: Map[String, String],
                       serdeParams: Map[String, String],
                       params: Map[String, String],
                       inputFormat: String,
                       outputFormat: String,
                       serde: String,
                       retention: Int,
                       createTime: Long,
                       lastAccessTime: Long,
                       owner: String)

}
