package io.eels.component.orc

import io.eels.schema.Schema
import org.apache.hadoop.hive.ql.io.orc.OrcStruct
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory

object OrcStructInspector {
  fun apply(schema: Schema): StructObjectInspector {
    val types: List<TypeInfo> = (0..schema.fields.size).map { TypeInfoFactory.stringTypeInfo }
    val typeInfo = TypeInfoFactory.getStructTypeInfo(schema.fieldNames(), types)
    return OrcStruct.createObjectInspector(typeInfo) as SettableStructObjectInspector
  }
}

object StandardStructInspector {

  fun apply(schema: Schema): StandardStructObjectInspector {

    val fieldInspectors: List<ObjectInspector> = (0..schema.fields.size).map { PrimitiveObjectInspectorFactory.javaStringObjectInspector }

    return ObjectInspectorFactory.getStandardStructObjectInspector(
        schema.fieldNames(),
        fieldInspectors
    )
  }
}
