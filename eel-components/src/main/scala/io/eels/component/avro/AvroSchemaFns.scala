package io.eels.component.avro

import com.sksamuel.exts.Logging
import io.eels.schema._
import org.apache.avro.{Schema, SchemaBuilder}
import org.codehaus.jackson.node.NullNode

import scala.collection.JavaConverters._

object AvroSchemaFns extends Logging {

  def toAvroSchema(structType: StructType,
                   caseSensitive: Boolean = true,
                   name: String = "row",
                   namespace: String = "namespace"): org.apache.avro.Schema = {

    def toAvroSchema(dataType: DataType): org.apache.avro.Schema = dataType match {
      case ArrayType(elementType) => SchemaBuilder.array().items(toAvroSchema(elementType))
      case BigIntType => SchemaBuilder.builder().longType()
      case BooleanType => SchemaBuilder.builder().booleanType()
      case DateType => SchemaBuilder.builder().longType()
      case DecimalType(precision, scale) => SchemaBuilder.builder().doubleType()
      case DoubleType => SchemaBuilder.builder().doubleType()
      case FloatType => SchemaBuilder.builder().floatType()
      case IntType(_) => SchemaBuilder.builder().intType()
      case LongType(_) => SchemaBuilder.builder().longType()
      case ShortType => SchemaBuilder.builder().intType()
      case StringType => SchemaBuilder.builder().stringType()
      case struct: StructType => toAvroSchema(struct)
      case TimestampType => SchemaBuilder.builder().longType()
    }

    def toAvroField(field: Field, caseSensitive: Boolean = true): org.apache.avro.Schema.Field = {

      val avroSchema = toAvroSchema(field.dataType)
      val fieldName = if (caseSensitive) field.name else field.name.toLowerCase()

      if (field.nullable) {
        val union = SchemaBuilder.unionOf().nullType().and().`type`(avroSchema).endUnion()
        new org.apache.avro.Schema.Field(fieldName, union, null, NullNode.getInstance())
      } else {
        new org.apache.avro.Schema.Field(fieldName, avroSchema, null: String, null: Object)
      }
    }

    val fields = structType.fields.map { field => toAvroField(field, caseSensitive) }
    val schema = org.apache.avro.Schema.createRecord(name, null, namespace, false)
    schema.setFields(fields.asJava)
    schema
  }

  def fromAvroSchema(schema: Schema, forceNullables: Boolean = false): StructType = {
    require(schema.getType == Schema.Type.RECORD)

    def fromAvroField(field: org.apache.avro.Schema.Field): Field = {
      val nullable = forceNullables ||
        field.schema.getType == Schema.Type.UNION &&
          field.schema.getTypes.asScala.map(_.getType).contains(Schema.Type.NULL)
      Field(field.name, toDataType(field.schema), nullable)
    }

    def toDataType(schema: Schema): DataType = schema.getType match {
      case org.apache.avro.Schema.Type.ARRAY => ArrayType.cached(toDataType(schema.getElementType))
      case org.apache.avro.Schema.Type.BOOLEAN => BooleanType
      case org.apache.avro.Schema.Type.BYTES => BinaryType
      case org.apache.avro.Schema.Type.DOUBLE => DoubleType
      case org.apache.avro.Schema.Type.ENUM => StringType
      case org.apache.avro.Schema.Type.FIXED => StringType
      case org.apache.avro.Schema.Type.FLOAT => FloatType
      case org.apache.avro.Schema.Type.INT => IntType.Signed
      case org.apache.avro.Schema.Type.LONG => LongType.Signed
      case org.apache.avro.Schema.Type.RECORD => StructType(schema.getFields.asScala.map(fromAvroField).toList)
      case org.apache.avro.Schema.Type.STRING => StringType
      case org.apache.avro.Schema.Type.UNION =>
        toDataType(schema.getTypes.asScala.filterNot(_.getType == Schema.Type.NULL).head)
    }

    toDataType(schema).asInstanceOf[StructType]
  }
}