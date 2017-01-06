package io.eels.component.orc

import io.eels.schema._
import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category

import scala.collection.JavaConverters._

object OrcSchemaFns {

  def toOrcSchema(structType: StructType): TypeDescription = {
    val struct = TypeDescription.createStruct()
    structType.fields.map { field =>
      val desc = field.dataType match {
        case BinaryType => TypeDescription.createBinary()
        case BooleanType => TypeDescription.createBoolean()
        case ByteType(_) => TypeDescription.createByte()
        case CharType(size) => TypeDescription.createChar().withMaxLength(size)
        case DateType => TypeDescription.createDate()
        case DecimalType(p, s) => TypeDescription.createDecimal().withScale(s.value).withPrecision(p.value)
        case DoubleType => TypeDescription.createDouble()
        case FloatType => TypeDescription.createFloat()
        case IntType(_) => TypeDescription.createInt()
        case LongType(_) => TypeDescription.createLong()
        case ShortType(_) => TypeDescription.createShort()
        case StringType => TypeDescription.createString()
        case TimestampMillisType => TypeDescription.createTimestamp()
        case VarcharType(size) => TypeDescription.createVarchar().withMaxLength(size)
      }
      struct.addField(field.name, desc)
    }
    struct
  }

  def fromOrcSchema(schema: TypeDescription): StructType = {
    val fields: Seq[Field] = schema.getChildren.asScala.zip(schema.getFieldNames.asScala).map { case (field, name) =>
      val datatype: DataType = field.getCategory match {
        case Category.BINARY => BinaryType
        case Category.BOOLEAN => BooleanType
        case Category.BYTE => ByteType.Signed
        case Category.CHAR => CharType(field.getMaxLength)
        case Category.DATE => DateType
        case Category.DECIMAL => DecimalType(Precision(field.getPrecision), Scale(field.getScale))
        case Category.DOUBLE => DoubleType
        case Category.FLOAT => FloatType
        case Category.INT => IntType.Signed
        case Category.LONG => LongType.Signed
        case Category.SHORT => ShortType.Signed
        case Category.STRING => StringType
        case Category.TIMESTAMP => TimestampMillisType
        case Category.VARCHAR => VarcharType(field.getMaxLength)
      }
      Field(name, datatype)
    }
    StructType(fields)
  }
}
