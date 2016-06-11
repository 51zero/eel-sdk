package io.eels.component.hive

import java.nio.file.Path
import java.util.Date

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import io.eels.schema.Schema
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient

object HiveSpecFn {

  def toSchemas(spec: HiveSpec): Map[String, Schema] = {
    spec.tables.map { table =>
      val columns = table.fields.map { field =>
        val (schemaType, precision, scale) = HiveSchemaFns.toSchemaType(field.`type`)
        Column(field.name, schemaType, true, precision, scale, true, None)
      }
      table.tableName -> Schema(columns)
    }.toMap
  }

  def toHiveSpec(dbName: String, tableName: String)
                (implicit fs: FileSystem, client:IMetaStoreClient): HiveSpec = {
    val tableSpecs = client.getAllTables(dbName).asScala.map { tableName =>
      val table = client.getTable(dbName, tableName)
      val location = table.getSd.getLocation
      val tableType = table.getTableType
      val partitions = client.listPartitions(dbName, tableName, Short.MaxValue).asScala.map { partition =>
        PartitionSpec(
          partition.getValues.asScala.toList,
          partition.getSd.getLocation,
          partition.getParameters.asScala.toMap.filterKeys(_ != "transient_lastDdlTime")
        )
      }.toList
      val columns = table.getSd.getCols.asScala.map(HiveSchemaFns.fromHiveField(_, true)).toList.map { column =>
        HiveFieldSpec(column.name, HiveSchemaFns.toHiveType(column), column.comment)
      }
      val owner = table.getOwner
      val retention = table.getRetention
      val createTime = table.getCreateTime
      val createTimeFormatted = new Date(createTime).toString
      val inputFormat = table.getSd.getInputFormat
      val outputFormat = table.getSd.getOutputFormat
      val serde = table.getSd.getSerdeInfo.getSerializationLib
      val params = table.getParameters.asScala.toMap.filterKeys(_ != "transient_lastDdlTime")
      val partitionKeys = table.getPartitionKeys.asScala.map(_.getName).toList
      HiveTableSpec(tableName, location, columns, tableType, partitionKeys, partitions, params, inputFormat, outputFormat, serde, retention, createTime, createTimeFormatted, owner)
    }
    HiveSpec(dbName, tableSpecs.toList)
  }
}

case class HiveSpec(dbName: String, tables: List[HiveTableSpec])

object HiveSpec {

  private val mapper = new ObjectMapper with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
  private val writer = mapper.writerWithDefaultPrettyPrinter()

  def apply(path: Path): HiveSpec = apply(Source.fromFile(path.toFile).getLines.mkString("\n"))
  def apply(str: String): HiveSpec = mapper.readValue[HiveSpec](str)

  def writeAsJson(spec: HiveSpec): String = writer.writeValueAsString(spec)
}

case class HiveTableSpec(tableName: String,
                         location: String,
                         fields: List[HiveFieldSpec],
                         tableType: String,
                         partitionKeys: List[String],
                         partitions: List[PartitionSpec],
                         params: Map[String, String],
                         inputFormat: String,
                         outputFormat: String,
                         serde: String,
                         retention: Int,
                         createTime: Long,
                         createTimeFormatted: String,
                         owner: String)

case class PartitionSpec(values: List[String], location: String, params: Map[String, String])

case class HiveFieldSpec(name: String,
                         `type`: String,
                         comment: Option[String] = None)