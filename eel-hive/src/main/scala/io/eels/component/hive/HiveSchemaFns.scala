package io.eels.component.hive

import com.sksamuel.exts.Logging
import io.eels.schema._
import org.apache.hadoop.hive.metastore.api.FieldSchema

// createReader FrameSchema from hive FieldSchemas
// see https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
object HiveSchemaFns extends Logging {

  val CharRegex = "char\\((.*?)\\)".r
  val VarcharRegex = "varchar\\((.*?)\\)".r
  val DecimalRegex = "decimal\\((\\d+),(\\d+)\\)".r
  val StructRegex = "struct<(.*?)>".r

  // everything up to the type seperator, then letters (which is the datatype), with an optional type params
  val StructElementRegex = "(.*?)\\:([a-z]+)(\\(.*?\\))?,?".r

  val ArrayRegex = "array<(.*?)>".r

  def fromHiveField(fieldSchema: FieldSchema): Field =
    fromHive(fieldSchema.getName, fieldSchema.getType, fieldSchema.getComment)

  def fromHive(name: String, typeInfo: String, comment: String): Field = {
    val dataType = fromHiveType(typeInfo)
    Field(name, dataType, true).withComment(comment)
  }

  // converts a hive type string into an eel DataType.
  def fromHiveType(descriptor: String): DataType = descriptor match {
    case ArrayRegex(element) =>
      val elementType = fromHiveType(element)
      ArrayType(elementType)
    case "bigint" => BigIntType
    case "binary" => BinaryType
    case "boolean" => BooleanType
    case CharRegex(size) => CharType(size.toInt)
    case DecimalRegex(precision, scale) => DecimalType(Precision(precision.toInt), Scale(scale.toInt))
    case "date" => DateType
    case "double" => DoubleType
    case "float" => FloatType
    case "int" => IntType.Signed
    case "smallint" => ShortType.Signed
    case "string" => StringType
    case "timestamp" => TimestampMillisType
    case "tinyint" => ByteType.Signed
    case StructRegex(struct) =>
      val fields = StructElementRegex.findAllMatchIn(struct).map { pattern =>
        val name = pattern.group(1)
        val datatypeString = pattern.group(2) + Option(pattern.group(3)).getOrElse("")
        val dataType = fromHiveType(datatypeString)
        Field(name, dataType, true)
      }
      StructType(fields.toVector)
    case VarcharRegex(size) => VarcharType(size.toInt)
    case _ => sys.error(s"Unsupported hive type [$descriptor]")
  }

  // converts an eel Schema into a seq of hive FieldSchema's
  def toHiveFields(schema: StructType): Vector[FieldSchema] = schema.fields.map(toHiveField)

  // converts an eel field into a hive FieldSchema
  def toHiveField(field: Field): FieldSchema = new FieldSchema(field.name.toLowerCase(), toHiveType(field), field.comment.orNull)

  /**
    * Returns the hive field (the type) for the given eel field
    */
  def toHiveType(field: Field): String = toHiveType(field.dataType)

  def toHiveType(dataType: DataType): String = dataType match {
    case ArrayType(elementType) => "array<" + toHiveType(elementType) + ">"
    case BigIntType => "bigint"
    case BinaryType => "binary"
    case _: ByteType => "tinyint"
    case BooleanType => "boolean"
    case CharType(size) => s"varchar($size)"
    case DateType => "date"
    case DecimalType(precision, scale) => s"decimal(${precision.value},${scale.value})"
    case DoubleType => "double"
    case EnumType(name, values) =>
      logger.warn("Hive does not support enum types; this field will be written as a varchar(255)")
      "varchar(255)"
    case FloatType => "float"
    case _: IntType => "int"
    case _: LongType => "bigint"
    case _: ShortType => "smallint"
    case StringType => "string"
    case TimestampMillisType => "timestamp"
    case TimestampMillisType => "timestamp"
    case VarcharType(size) => s"varchar($size)"
    case _ => sys.error(s"No conversion from eel type [$dataType] to hive type")
  }

  def toStructDDL(fields: Vector[Field]): String = {
    val types = fields.map { it => it.name + ":" + toHiveType(it) }.mkString(",")
    s"struct<$types>"
  }
}
