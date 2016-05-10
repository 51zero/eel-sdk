package io.eels.component.avro

import io.eels.Column
import io.eels.ColumnType
import io.eels.Schema
import org.apache.avro.SchemaBuilder

import io.eels.component.avro.avroSchemaToSchema
import io.eels.component.avro.schemaToAvroSchema
import io.kotlintest.specs.WordSpec
import org.codehaus.jackson.node.NullNode
import java.util.*

class AvroSchemaFnTest : WordSpec() {

  init {
    "toAvro" should {
      "use a union of [null, type] for a nullable column" with {
        val schema = Schema(Column("a", ColumnType.String, true))
        val fields = schemaToAvroSchema(schema).fields
        fields[0].schema().type shouldBe org.apache.avro.Schema.Type.UNION
        fields[0].schema().types[0].type shouldBe org.apache.avro.Schema.Type.NULL
        fields[0].schema().types[1].type shouldBe org.apache.avro.Schema.Type.STRING
      }
      "set default type of NullNode for a nullable column" with {
        val schema = Schema(Column("a", ColumnType.String, true))
        val fields = schemaToAvroSchema(schema).fields
        fields[0].defaultValue() shouldBe NullNode.getInstance()
      }
      "not set a default value for a non null column" with {
        val schema = Schema(Column("a", ColumnType.Int, false))
        val fields = schemaToAvroSchema(schema).fields
        (fields[0].defaultValue() == null) shouldBe true
        fields[0].schema().type shouldBe org.apache.avro.Schema.Type.INT
      }
    }

    "fromAvro" should {
      "convert avro unions [null, string] to nullable columns" with {
        val avro = SchemaBuilder.record("dummy").fields().optionalString("str").endRecord()
        avroSchemaToSchema(avro) shouldBe Schema(Column("str", ColumnType.String, true, 0, 0, true))
      }
      "convert avro unions [null, enum] to nullable columns" with {
        val enum = org.apache.avro.Schema.createEnum("myenum", null, null, Arrays.asList("a", "b"))
        val union = org.apache.avro.Schema.createUnion(Arrays.asList(enum, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL)))
        val avro = SchemaBuilder.record("dummy").fields().name("u").`type`(union).noDefault().endRecord()
        avroSchemaToSchema(avro) shouldBe Schema(Column("u", ColumnType.String, true, 0, 0, true))
      }
      "convert avro unions [null, double] to nullable double columns" with {
        val union = org.apache.avro.Schema.createUnion(Arrays.asList(SchemaBuilder.builder().doubleType(), SchemaBuilder.builder().nullType()))
        val avro = SchemaBuilder.record("dummy").fields().name("u").`type`(union).noDefault().endRecord()
        avroSchemaToSchema(avro) shouldBe Schema(Column("u", ColumnType.Double, true, 0, 0, true))
      }
      "convert avro boolean fields" with {
        val avro = SchemaBuilder.record("dummy").fields().requiredBoolean("b").endRecord()
        avroSchemaToSchema(avro) shouldBe Schema(Column("b", ColumnType.Boolean, false, 0, 0, true))
      }
      "convert avro float fields" with {
        val avro = SchemaBuilder.record("dummy").fields().requiredFloat("f").endRecord()
        avroSchemaToSchema(avro) shouldBe Schema(Column("f", ColumnType.Float, false, 0, 0, true))
      }
      "convert avro double fields" with {
        val avro = SchemaBuilder.record("dummy").fields().requiredDouble("d").endRecord()
        avroSchemaToSchema(avro) shouldBe Schema(Column("d", ColumnType.Double, false, 0, 0, true))
      }
      "convert avro fixed fields to string" with {
        val fixed = org.apache.avro.Schema.createFixed("dummy", null, null, 123)
        val avro = SchemaBuilder.record("dummy").fields().name("f").`type`(fixed).noDefault().endRecord()
        avroSchemaToSchema(avro) shouldBe Schema(Column("f", ColumnType.String, false, 0, 0, true))
      }
      "convert avro enum fields to string" with {
        val enum = org.apache.avro.Schema.createEnum("myenum", null, null, Arrays.asList("a", "b"))
        val avro = SchemaBuilder.record("dummy").fields().name("e").`type`(enum).noDefault().endRecord()
        avroSchemaToSchema(avro) shouldBe Schema(Column("e", ColumnType.String, false, 0, 0, true))
      }
      "convert avro int fields" with {
        val avro = SchemaBuilder.record("dummy").fields().requiredInt("i").endRecord()
        avroSchemaToSchema(avro) shouldBe Schema(Column("i", ColumnType.Int, false, 0, 0, true))
      }
      "convert avro long fields" with {
        val avro = SchemaBuilder.record("dummy").fields().requiredLong("l").endRecord()
        avroSchemaToSchema(avro) shouldBe Schema(Column("l", ColumnType.Long, false, 0, 0, true))
      }
      "convert avro string fields" with {
        val avro = SchemaBuilder.record("dummy").fields().requiredString("s").endRecord()
        avroSchemaToSchema(avro) shouldBe Schema(Column("s", ColumnType.String, false, 0, 0, true))
      }
    }
  }
}


