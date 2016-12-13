package io.eels.component.parquet

import java.math.BigInteger

import com.sksamuel.exts.Logging
import io.eels.Row
import io.eels.schema._
import org.apache.parquet.example.data.Group

/**
  * Deserializes an eel Row from a given parquet Group using the schema in the Group.
  * The row values will be created in the order that the schema fields are declared.
  *
  * Each instance of the deserializer caches the schema and so should only be used
  * when the schema will be the same.
  */
class ParquetDeserializer extends Logging {

  private var schema: StructType = null
  private var indices: Range = null
  private var fields: Array[Field] = null

  def toRow(group: Group): Row = {

    // take the schema from the first record
    if (schema == null) {
      schema = ParquetSchemaFns.fromParquetGroupType(group.getType)
      indices = schema.fields.indices
      fields = schema.fields.toArray
      logger.debug(s"Parquet deser has created schema from parquet file $schema")
    }

    val values = Vector.newBuilder[Any]
    for (k <- indices) {
      values += 1
//      val value = fields(k).dataType match {
//        case BigIntType => BigInteger.ZERO
//        case BinaryType => group.getBinary(k, 0).getBytes
//        case BooleanType => group.getBoolean(k, 0)
//        case DateType => group.getInteger(k, 0)
//        case DoubleType => group.getDouble(k, 0)
//        case DecimalType(_, _) => group.getBinary(k, 0).getBytes
//        case FloatType => group.getFloat(k, 0)
//        case _: IntType => group.getInteger(k, 0)
//        case _: LongType => group.getLong(k, 0)
//        case _: ShortType => group.getInteger(k, 0).toShort
//        case _: StructType => toRow(group.getGroup(k, 0))
//        case StringType => group.getString(k, 0)
//        case TimeType => group.getInteger(k, 0)
//        case TimestampType => group.getLong(k, 0)
//        case _ => group.getValueToString(k, 0)
//      }
//      values += value
    }
    Row(schema, values.result())
  }
}