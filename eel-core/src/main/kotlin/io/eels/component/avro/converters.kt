package io.eels.component.avro

/**
 * Avro requires that the types of values passed to its JavaAPI match the types in the schema.
 * So if you have an Avro Boolean field, you can't pass "true" but it must be true. So it's less
 * "forgiving" than JDBC for example.
 *
 * An AvroConverter instance will convert an incoming type into the a type appropriate for
 * the output type that the implementation supports.
 */
interface AvroConverter<T> {
  fun convert(value: Any): T
}

object StringConverter : AvroConverter<String> {
  override fun convert(value: Any): String = value.toString()
}

object BooleanConverter : AvroConverter<Boolean> {
  override fun convert(value: Any): Boolean = when (value) {
    is Boolean -> value
    "true" -> true
    "false" -> false
    else -> error("Could not convert $value to Boolean")
  }
}

object IntConverter : AvroConverter<Int> {
  override fun convert(value: Any): Int = when (value) {
    is Int -> value
    is Long -> value.toInt()
    else -> value.toString().toInt()
  }
}

object ShortConverter : AvroConverter<Short> {
  override fun convert(value: Any): Short = when (value) {
    is Short -> value
    is Int -> value.toShort()
    is Long -> value.toShort()
    else -> value.toString().toShort()
  }
}

object LongConverter : AvroConverter<Long> {
  override fun convert(value: Any): Long = when (value) {
    is Long -> value
    is Short -> value.toLong()
    is Int -> value.toLong()
    else -> value.toString().toLong()
  }
}

object DoubleConverter : AvroConverter<Double> {
  override fun convert(value: Any): Double = when (value) {
    is Double -> value
    is Float -> value.toDouble()
    is Long -> value.toDouble()
    is Int -> value.toDouble()
    else -> value.toString().toDouble()
  }
}

object FloatConverter : AvroConverter<Float> {
  override fun convert(value: Any): Float = when (value) {
    is Float -> value
    is Double -> value.toFloat()
    is Long -> value.toFloat()
    is Int -> value.toFloat()
    else -> value.toString().toFloat()
  }
}