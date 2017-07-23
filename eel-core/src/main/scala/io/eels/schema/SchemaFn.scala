package io.eels.schema

import java.sql.Timestamp

object SchemaFn {
  def toDataType(clz: Class[_]): DataType = {
    val intClass = classOf[Int]
    val floatClass = classOf[Float]
    val stringClass = classOf[String]
    val charClass = classOf[Char]
    val bigIntClass = classOf[BigInt]
    val booleanClass = classOf[Boolean]
    val doubleClass = classOf[Double]
    val bigdecimalClass = classOf[BigDecimal]
    val longClass = classOf[Long]
    val byteClass = classOf[Byte]
    val shortClass = classOf[Short]
    val timestampClass = classOf[Timestamp]
    clz match {
      case `bigdecimalClass` => DecimalType(Precision(22), Scale(5))
      case `bigIntClass` => BigIntType
      case `booleanClass` => BooleanType
      case `byteClass` => ByteType.Signed
      case `charClass` => CharType(1)
      case `doubleClass` => DoubleType
      case `intClass` => IntType.Signed
      case `floatClass` => FloatType
      case `longClass` => LongType.Signed
      case `stringClass` => StringType
      case `shortClass` => ShortType.Signed
      case `timestampClass` => TimestampMillisType
      case _ => sys.error(s"Can not map $clz to data type")
    }
  }
}