// create FrameSchema from hive FieldSchemas
// see https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
object HiveSchemaFns extends StrictLogging {

  def toHiveField(column: Column): FieldSchema = new FieldSchema(column.name.toLowerCase, toHiveType(column), null)
  def toHiveFields(schema: Schema): Seq[FieldSchema] = toHiveFields(schema.columns)
  def toHiveFields(columns: Seq[Column]): Seq[FieldSchema] = columns.map(toHiveField)

  def fromHiveField(s: FieldSchema, nullable: Boolean): Column = {
    val (schemaType, precision, scale) = toSchemaType(s.getType)
    Column(s.getName, schemaType, nullable, precision = precision, scale = scale, comment = NonEmptyString(s.getComment))
  }

  val VarcharRegex = "varchar\\((.*?)\\)".r
  val DecimalRegex = "decimal\\((\\d+),(\\d+)\\)".r

  type Scale = Int
  type Precision = Int

  def toSchemaType(str: String): (SchemaType, Precision, Scale) = str match {
    case "tinyint" => (SchemaType.Short, 0, 0)
    case "smallint" => (SchemaType.Short, 0, 0)
    case "int" => (SchemaType.Int, 0, 0)
    case "boolean" => (SchemaType.Boolean, 0, 0)
    case "bigint" => (SchemaType.BigInt, 0, 0)
    case "float" => (SchemaType.Float, 0, 0)
    case "double" => (SchemaType.Double, 0, 0)
    case "string" => (SchemaType.String, 0, 0)
    case "binary" => (SchemaType.Binary, 0, 0)
    case "char" => (SchemaType.String, 0, 0)
    case "date" => (SchemaType.Date, 0, 0)
    case "timestamp" => (SchemaType.Timestamp, 0, 0)
    case DecimalRegex(precision, scale) => (SchemaType.Decimal, precision.toInt, scale.toInt)
    case VarcharRegex(precision) => (SchemaType.String, precision.toInt, 0)
    case other =>
      logger.warn(s"Unknown schema type $other; defaulting to string")
      (SchemaType.String, 0, 0)
  }


  /**
    * Returns the hive column type for the given column
    */
  def toHiveType(column: Column): String = column.`type` match {
    case SchemaType.BigInt => "bigint"
    case SchemaType.Boolean => "boolean"
    case SchemaType.Decimal => s"decimal(${column.scale},${column.precision})"
    case SchemaType.Double => "double"
    case SchemaType.Float => "float"
    case SchemaType.Int => "int"
    case SchemaType.Long => "bigint"
    case SchemaType.Short => "smallint"
    case SchemaType.String => "string"
    case SchemaType.Timestamp => "timestamp"
    case SchemaType.Date => "date"
    case _ =>
      logger.warn(s"No conversion from schema type ${column.`type`} to hive type; defaulting to string")
      "string"
  }
}