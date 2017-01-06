package io.eels.schema

object SchemaFn {
  def toFieldType(clz: Class[_]): DataType = {
    val intClass = classOf[Int]
    val floatClass = classOf[Float]
    val stringClass = classOf[String]
    val charClass = classOf[Char]
    val bigIntClass = classOf[BigInt]
    val booleanClass = classOf[Boolean]
    val doubleClass = classOf[Double]
    val bigdecimalClass = classOf[BigDecimal]
    val longClass = classOf[Long]
    clz match {
      case `bigdecimalClass` => DecimalType(Precision(18), Scale(2))
      case `bigIntClass` => BigIntType
      case `booleanClass` => BooleanType
      case `doubleClass` => DoubleType
      case `intClass` => IntType.Signed
      case `floatClass` => FloatType
      case `charClass` => CharType(1)
      case `longClass` => LongType.Signed
      case `stringClass` => StringType
      case _ => sys.error(s"Can not map $clz to FieldType")
    }
  }
}