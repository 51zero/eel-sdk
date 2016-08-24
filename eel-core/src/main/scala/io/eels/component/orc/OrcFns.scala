package io.eels.component.orc

import io.eels.schema.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcFile.ReaderOptions
import org.apache.orc.{OrcFile, TypeDescription}

import scala.collection.JavaConverters._

object OrcFns {

  def writeSchema(schema: Schema): TypeDescription = {
    val orcSchema = TypeDescription.createStruct()
    schema.fields.foreach { field =>
      orcSchema.addField(field.name, TypeDescription.createString())
    }
    orcSchema
  }

  def readSchema(schema: TypeDescription): Schema = Schema.fromFieldNames(schema.getFieldNames.asScala)

  def readSchema(path: Path)(implicit conf: Configuration): Schema = {
    val reader = OrcFile.createReader(path, new ReaderOptions(conf).maxLength(1))
    val schema = reader.getSchema()
    readSchema(schema)
  }
}