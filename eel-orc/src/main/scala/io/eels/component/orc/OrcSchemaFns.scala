package io.eels.component.orc

import io.eels.schema._
import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category

import scala.collection.JavaConverters._

object OrcSchemaFns {

  def toOrcSchema(dataType: DataType): TypeDescription = {
    dataType match {
      case ArrayType(elementType) => TypeDescription.createList(toOrcSchema(elementType))
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
      case MapType(keyType, valueType) => TypeDescription.createMap(toOrcSchema(keyType), toOrcSchema(valueType))
      case ShortType(_) => TypeDescription.createShort()
      case StringType => TypeDescription.createString()
      case StructType(fields) =>
        fields.foldLeft(TypeDescription.createStruct) { case (tpe, field) =>
          tpe.addField(field.name, toOrcSchema(field.dataType))
        }
      case TimestampMillisType => TypeDescription.createTimestamp()
      case VarcharType(size) => TypeDescription.createVarchar().withMaxLength(size)
      case unsupportedDataType =>
        throw new UnsupportedOperationException(s"type ${unsupportedDataType.toString} is not supported by ORC")
    }
  }

  def fromOrcType(tpe: TypeDescription): DataType = {
    tpe.getCategory match {
      case Category.BINARY => BinaryType
      case Category.BOOLEAN => BooleanType
      case Category.BYTE => ByteType.Signed
      case Category.CHAR => CharType(tpe.getMaxLength)
      case Category.DATE => DateType
      case Category.DECIMAL => DecimalType(Precision(tpe.getPrecision), Scale(tpe.getScale))
      case Category.DOUBLE => DoubleType
      case Category.FLOAT => FloatType
      case Category.INT => IntType.Signed
      case Category.LIST => ArrayType(fromOrcType(tpe.getChildren.get(0)))
      case Category.LONG => LongType.Signed
      case Category.MAP => MapType(fromOrcType(tpe.getChildren().get(0)), fromOrcType(tpe.getChildren.get(1)))
      case Category.SHORT => ShortType.Signed
      case Category.STRING => StringType
      case Category.STRUCT =>
        val fields = tpe.getFieldNames.asScala.zip(tpe.getChildren.asScala).map { case (name, subtype) =>
          Field(name, fromOrcType(subtype))
        }
        StructType(fields)
      case Category.TIMESTAMP => TimestampMillisType
      case Category.VARCHAR => VarcharType(tpe.getMaxLength)
    }
  }
}
