package io.eels.component.orc

import io.eels.schema.Schema
import org.apache.hadoop.hive.ql.io.orc.OrcStruct
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.typeinfo.{TypeInfo, TypeInfoFactory}
import scala.collection.JavaConverters._

object OrcStructInspector {
  def apply(schema: Schema): StructObjectInspector = {
    val types: java.util.List[TypeInfo] = schema.fields.indices.map { _ => TypeInfoFactory.stringTypeInfo }.asJava.asInstanceOf[java.util.List[TypeInfo]]
    val typeInfo = TypeInfoFactory.getStructTypeInfo(schema.fieldNames().asJava, types)
    OrcStruct.createObjectInspector(typeInfo).asInstanceOf[SettableStructObjectInspector]
  }
}

object StandardStructInspector {

  def apply(schema: Schema): StandardStructObjectInspector = {

    val fieldInspectors: java.util.List[ObjectInspector] = schema.fields.indices.map {
      _ => PrimitiveObjectInspectorFactory.javaStringObjectInspector
    }.asJava.asInstanceOf[java.util.List[ObjectInspector]]

    ObjectInspectorFactory.getStandardStructObjectInspector(
      schema.fieldNames().asJava,
        fieldInspectors
    )
  }
}
