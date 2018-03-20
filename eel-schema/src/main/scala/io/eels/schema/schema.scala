package io.eels.schema

import org.apache.commons.lang.StringUtils

import scala.language.implicitConversions
import scala.reflect.runtime.universe
import scala.util.matching.Regex

sealed trait DataType {
  def canonicalName: String = getClass.getSimpleName.toLowerCase.stripSuffix("$").stripSuffix("type")
  def matches(from: DataType): Boolean = this == from
}

object DataType {
  def apply(str: String): DataType = str match {
    case "byte" => ByteType.Signed
    case "ubyte" => ByteType.Unsigned
    case "short" => ShortType.Signed
    case "ushort" => ShortType.Unsigned
    case "int" => IntType.Signed
    case "uint" => IntType.Unsigned
    case "long" => LongType.Signed
    case "ulong" => LongType.Unsigned
    case "double" => DoubleType
    case "float" => FloatType
    case "bool" => BooleanType
    case "string" => StringType
  }
}

object BigIntType extends DataType
object BinaryType extends DataType
object BooleanType extends DataType {
  override def canonicalName: String = "bool"
}
object DateType extends DataType
object DoubleType extends DataType
object FloatType extends DataType
object StringType extends DataType

// a time without a date; number of milliseconds after midnight
object TimeMillisType extends DataType

// a time without a date; number of micros after midnight
object TimeMicrosType extends DataType

// number of millis since the unix epoch for UTC
object TimestampMillisType extends DataType

// number of micros since the unix epoch for UTC
object TimestampMicrosType extends DataType

case class EnumType(name: String, values: Seq[String]) extends DataType
object EnumType {
  def apply(name: String, first: String, rest: String*): EnumType = new EnumType(name, first +: rest)
}

case class ByteType(signed: Boolean = true) extends DataType {
  override def canonicalName: String = if (signed) "byte" else "ubyte"
}

object ByteType {
  val Signed = ByteType(true)
  val Unsigned = ByteType(false)
}

case class ShortType(signed: Boolean = true) extends DataType {
  override def canonicalName: String = if (signed) "short" else "ushort"
}

object ShortType {
  val Signed = ShortType(true)
  val Unsigned = ShortType(false)
}

case class IntType(signed: Boolean = true) extends DataType {
  override def canonicalName: String = if (signed) "int" else "uint"
}

object IntType {
  val Signed = IntType(true)
  val Unsigned = IntType(false)
}

case class LongType(signed: Boolean = true) extends DataType {
  override def canonicalName: String = if (signed) "long" else "ulong"
}

object LongType {
  val Signed = LongType(true)
  val Unsigned = LongType(false)
}

case class CharType(size: Int) extends DataType {
  override def canonicalName: String = s"char($size)"
}

case class VarcharType(size: Int) extends DataType {
  override def canonicalName: String = s"varchar($size)"
}

case class DecimalType(precision: Precision = Precision(0),
                       scale: Scale = Scale(0)) extends DataType {
  if (precision.value != -1)
    require(scale.value <= precision.value, s"Scale ${scale.value} should be less than or equal to precision ${precision.value}")
  override def canonicalName: String = "decimal(" + precision.value + "," + scale.value + ")"
  override def matches(from: DataType): Boolean = from match {
    case DecimalType(p, s) => (s == scale || s.value == -1 || scale.value == -1) && (p == precision || p.value == -1 || precision.value == -1)
    case _ => false
  }
}

object DecimalType {
  val MaxPrecision = 38
  val Wildcard = DecimalType(Precision(-1), Scale(-1))
  val Default = DecimalType(Precision(18), Scale(2))
}

case class ArrayType(elementType: DataType) extends DataType {
  override def canonicalName: String = "array<" + elementType.canonicalName + ">"
}

object ArrayType {

  val Doubles = ArrayType(DoubleType)
  val SignedInts = ArrayType(IntType.Signed)
  val SignedLongs = ArrayType(LongType.Signed)
  val Booleans = ArrayType(BooleanType)
  val Strings = ArrayType(StringType)

  def cached(elementType: DataType): ArrayType = elementType match {
    case DoubleType => ArrayType.Doubles
    case IntType.Signed => ArrayType.SignedInts
    case LongType.Signed => ArrayType.SignedLongs
    case BooleanType => ArrayType.Booleans
    case StringType => ArrayType.Strings
    case _ => ArrayType(elementType)
  }
}

case class Precision(value: Int) extends AnyVal
object Precision {
  implicit def intToPrecision(value: Int): Precision = Precision(value)
}

case class Scale(value: Int) extends AnyVal
object Scale {
  implicit def intToScale(value: Int): Scale = Scale(value)
}

case class StructType(fields: Vector[Field]) extends DataType {


  require(fields.nonEmpty, "StructType cannot be empty")
  private val dups = fields.map(_.name).groupBy(identity).collect { case (x, list) if list.size > 1 => x }
  if (dups.nonEmpty) {
    sys.error(s"StructType cannot have duplicated field names: ${dups.mkString(", ")}")
  }

  val size: Int = fields.size

  def apply(name: String): Option[Field] = fields.find(_.name == name)

  def indexOf(field: Field): Int = indexOf(field.name, true)
  def indexOf(field: Field, caseSensitive: Boolean): Int = indexOf(field.name, caseSensitive)

  def indexOf(fieldName: String): Int = indexOf(fieldName, true)
  def indexOf(fieldName: String, caseSensitive: Boolean): Int = {
    if (caseSensitive) {
      fields.indexWhere(_.name == fieldName)
    } else {
      fields.indexWhere(_.name.equalsIgnoreCase(fieldName))
    }
  }

  // returns the field names that match the given regex
  def fieldNames(regex: Regex): Seq[String] = fieldNames.filter { name => regex.pattern.matcher(name).matches() }

  def partitions: Seq[Field] = fields.filter(_.partition)

  def projection(fieldNames: Seq[String]): StructType = StructType(
    fieldNames.flatMap { name =>
      field(name)
    }.toList
  )

  def replaceFieldType(from: DataType, to: DataType): StructType = {
    StructType(fields.map {
      case field if field.dataType.matches(from) => field.copy(dataType = to)
      case field => field
    })
  }

  def replaceFieldType(regex: Regex, datatype: DataType): StructType = {
    StructType(fields.map {
      case field if regex.pattern.matcher(field.name).matches() => field.copy(dataType = datatype)
      case field => field
    })
  }

  def field(pos: Int): Field = fields.apply(pos)
  def field(name: String, caseSensitive: Boolean = true): Option[Field] = {
    if (caseSensitive) fields.find(_.name == name) else fields.find(_.name.toLowerCase == name.toLowerCase)
  }

  def toLowerCase(): StructType = copy(fields = fields.map(_.toLowerCase()))

  def fieldNames(): Seq[String] = fields.map(_.name)

  def addField(name: String): StructType = addField(Field(name, StringType))

  def addField(field: Field): StructType = {
    require(!fieldNames().contains(field.name), s"Field ${field.name} already exists")
    copy(fields = fields :+ field)
  }

  def contains(fieldName: String, caseSensitive: Boolean = true): Boolean = {
    def contains(fields: Seq[Field]): Boolean = fields.exists { it =>
      (if (caseSensitive) fieldName == it.name else fieldName equalsIgnoreCase it.name) || fields.map(_.dataType).collect {
        case struct: StructType => struct.fields
      }.exists(contains)
    }
    contains(fields)
  }

  def stripFromFieldNames(chars: Seq[Char]): StructType = {
    def strip(name: String): String = chars.foldLeft(name) { (name, char) => name.replace(char.toString, "") }
    StructType(fields.map { field =>
      field.copy(name = strip(field.name))
    })
  }

  def addFieldIfNotExists(name: String): StructType = if (fieldNames().contains(name)) this else addField(Field(name, StringType))
  def addFieldIfNotExists(field: Field): StructType = if (fieldNames().contains(field.name)) this else addField(field)

  def updateFieldType(fieldName: String, dataType: DataType): StructType = copy(fields = fields.map { field =>
    if (field.name == fieldName) field.copy(dataType = dataType)
    else field
  })

  def removeFields(regex: Regex): StructType = StructType(fields.filterNot { field => regex.pattern.matcher(field.name).matches })
  def removeFields(first: String, rest: String*): StructType = removeFields(first +: rest)
  def removeFields(names: Seq[String]): StructType = copy(fields = fields.filterNot { field =>
    names.contains(field.name)
  })

  def removeField(name: String, caseSensitive: Boolean = true): StructType = {
    copy(fields = fields.filterNot { field =>
      if (caseSensitive) field.name == name else field.name.equalsIgnoreCase(name)
    })
  }

  def concat(other: StructType): StructType = {
    require(
      fields.map(_.name).intersect(other.fields.map(_.name)).isEmpty,
      "Cannot join two structs which have common field names"
    )
    StructType(fields ++ other.fields)
  }

  def replaceField(sourceFieldName: String, targetField: Field): StructType = StructType(
    fields.map {
      case field if field.name == sourceFieldName => targetField
      case field => field
    }
  )

  def renameField(from: String, to: String): StructType = StructType(fields.map { field =>
    if (field.name == from) field.copy(name = to) else field
  })

  def show(): String = {
    "Struct\n" + fields.map { field =>
      val nullString = if (field.nullable) "nullable" else "not nullable"
      val partitionString = if (field.partition) "partition" else ""
      s"- ${field.name} [${field.dataType} $nullString $partitionString]"
    }.mkString("\n")
  }

  def ddl(table: String): String = {
    s"CREATE TABLE $table " + fields.map { it =>
      it.name + " " + it.dataType.toString.toLowerCase.stripSuffix("type")
    }.mkString("(", ", ", ")")
  }
}

object StructType {

  def fromFieldNames(names: Seq[String]): StructType = apply(names.map(Field.apply(_, StringType)))

  def apply(fields: Seq[Field]): StructType = new StructType(fields.toVector)
  def apply(first: Field, rest: Field*): StructType = new StructType((first +: rest).toVector)
  def apply(first: String, rest: String*): StructType = new StructType((first +: rest).map(name => Field(name, StringType)).toVector)

  import scala.reflect.runtime.universe._

  def from[T <: Product : TypeTag](implicit fieldNameStrategy: FieldNameStrategy = JvmFieldNameStrategy): StructType = {

    def struct(tpe: universe.Type): StructType = {
      val fields = tpe.declarations.collect {
        case m: MethodSymbol if m.isCaseAccessor =>
          val dataType = process(m.returnType)
          val nullable = m.returnType <:< typeOf[Option[Any]]
          Field(fieldNameStrategy.fieldName(m.name.toString), dataType, nullable)
      }
      StructType(fields.toList)
    }

    def process(tpe: universe.Type): DataType = {

      if (tpe <:< typeOf[scala.collection.Seq[Any]]) {
        // val dataType = process(tpe.typeConstructor..head)
        ArrayType(StringType)
      } else if (tpe <:< typeOf[Option[Any]]) {
        process(tpe.typeArgs(0))
      } else if (tpe <:< typeOf[Product]) {
        struct(tpe)
      } else {
        val javaClass = implicitly[TypeTag[T]].mirror.runtimeClass(tpe)
        SchemaFn.toDataType(javaClass)
      }
    }

    struct(typeOf[T])
  }
}

trait FieldNameStrategy {
  def fieldName(jvmName: String): String
}

object JvmFieldNameStrategy extends FieldNameStrategy {
  override def fieldName(jvmName: String): String = jvmName
}

object SnakeCaseFieldNameStrategy extends FieldNameStrategy {
  override def fieldName(jvmName: String): String = StringUtils.splitByCharacterTypeCamelCase(jvmName).map(_.toLowerCase).mkString("_")
}

case class MapType(keyType: DataType, valueType: DataType) extends DataType