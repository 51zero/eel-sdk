package io.eels.component.hive

import io.eels.schema.{Field, StructType}
import org.apache.hadoop.hive.metastore.TableType

object HiveDDL {

  def showDDL(tableName: String,
              fields: Seq[Field],
              tableType: TableType = TableType.MANAGED_TABLE,
              partitions: Seq[String] = Nil,
              location: Option[String] = None,
              serde: String = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
              inputFormat: String = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
              outputFormat: String = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
              props: Map[String, String] = Map.empty,
              tableComment: Option[String] = None,
              ifNotExists: Boolean = true): String = {

    val create = s"CREATE ${if (tableType == TableType.EXTERNAL_TABLE) "EXTERNAL " else ""}TABLE ${if (ifNotExists) "IF NOT EXISTS" else ""} `$tableName` ("

    val fls = fields.map { field =>
      val hiveType = HiveSchemaFns.toHiveType(field)
      val comment = field.comment.map(x => " " + x).getOrElse("")
      s"`${field.name.toLowerCase}` $hiveType$comment"
    }.mkString("   ", ",\n   ", ")")

    val formats = s"ROW FORMAT SERDE\n   '$serde'\nSTORED AS INPUTFORMAT\n   '$inputFormat'\nOUTPUTFORMAT\n   '$outputFormat'"
    val loc = location.map(x => s"LOCATION '$x'")

    val parts = if (partitions.isEmpty)
      None
    else
      Some(partitions.map(x => s"`$x` string").mkString("PARTITIONED BY (\n   ", ",\n   ", ")"))

    val tblprops = if (props.isEmpty)
      None
    else
      Some(props.map { case (key, value) => s"'$key'='$value'" }.mkString("TBLPROPERTIES (", ",", ")"))

    (Seq(create, fls) ++ Seq(parts, loc, Some(formats), tblprops).flatten).mkString("\n")
  }

  implicit class HiveDDLOps(schema: StructType) {

    def showDDL(tableName: String,
                tableType: TableType = TableType.MANAGED_TABLE,
                partitions: Seq[String] = Nil,
                location: Option[String] = None,
                format: HiveFormat,
                props: Map[String, String] = Map.empty,
                tableComment: Option[String] = None,
                ifNotExists: Boolean = true) = {
      HiveDDL.showDDL(tableName,
        schema.fields,
        tableType,
        partitions,
        location,
        format.serde,
        format.inputFormat,
        format.outputFormat,
        props,
        tableComment,
        ifNotExists)
    }
  }
}