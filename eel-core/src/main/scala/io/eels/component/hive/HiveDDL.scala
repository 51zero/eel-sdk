package io.eels.component.hive

import io.eels.schema.{Field, Schema}
import org.apache.hadoop.hive.metastore.TableType

object HiveDDL {

  def showDDL(tableName: String,
              fields: Seq[Field],
              tableType: TableType = TableType.MANAGED_TABLE,
              partitions: Seq[String] = Nil,
              location: Option[String] = None,
              inputFormat: String = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
              outputFormat: String = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
              props: Map[String, String] = Map.empty,
              tableComment: Option[String] = None,
              ifNotExists: Boolean = true): String = {

    val create = s"CREATE ${if (tableType == TableType.EXTERNAL_TABLE) "EXTERNAL " else ""}TABLE ${if (ifNotExists) "IF NOT EXISTS" else ""} $tableName"

    val fls = fields.map { field =>
      val hiveType = HiveSchemaFns.toHiveType(field)
      val comment = field.comment.getOrElse("")
      s"${field.name.toLowerCase} $hiveType $comment"
    }.mkString("(", ", ", ")")

    val loc = location.map(x => s"LOCATION '$x'")
    val i = Some(s"INPUTFORMAT '$inputFormat'")
    val o = Some(s"OUTPUTFORMAT '$outputFormat'")

    val parts = if (partitions.isEmpty)
      None
    else
      Some(partitions.mkString("PARTITIONED BY (", " string, ", " string)"))

    val tblprops = if (props.isEmpty)
      None
    else
      Some(props.map { case (key, value) => s"'$key'='$value'" }.mkString("TBLPROPERTIES (", ",", ")"))

    (Seq(create, fls) ++ Seq(parts, loc, i, o, tblprops).flatten).mkString("\n")
  }

  implicit class HiveDDLOps(schema: Schema) {
    def showDDL(tableName: String,
                tableType: TableType = TableType.MANAGED_TABLE,
                partitions: Seq[String] = Nil,
                location: Option[String] = None,
                inputFormat: String = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                outputFormat: String = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                props: Map[String, String] = Map.empty,
                tableComment: Option[String] = None,
                ifNotExists: Boolean = true) = {
      HiveDDL.showDDL(tableName,
        schema.fields,
        tableType,
        partitions,
        location,
        inputFormat,
        outputFormat,
        props,
        tableComment,
        ifNotExists)
    }
  }
}