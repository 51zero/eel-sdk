package io.eels.component.orc

import io.eels.FrameSchema
import org.apache.hadoop.hive.ql.io.orc.OrcStruct
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{StandardStructObjectInspector, ObjectInspectorFactory, ObjectInspector, StructObjectInspector, SettableStructObjectInspector}
import org.apache.hadoop.hive.serde2.typeinfo.{TypeInfoFactory, TypeInfo}
import scala.collection.JavaConverters._

object OrcStructInspector {
  def apply(schema: FrameSchema): StructObjectInspector = {
    val types: List[TypeInfo] = List.fill(schema.columns.size)(TypeInfoFactory.stringTypeInfo)
    val typeInfo = TypeInfoFactory.getStructTypeInfo(schema.columnNames.asJava, types.asJava)
    OrcStruct.createObjectInspector(typeInfo).asInstanceOf[SettableStructObjectInspector]
  }
}

object StandardStructInspector {
  def apply(schema: FrameSchema): StandardStructObjectInspector = {
    val fieldInspectors: List[ObjectInspector] = {
      List.fill(schema.columns.size)(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    }

    ObjectInspectorFactory.getStandardStructObjectInspector(
      schema.columnNames.asJava,
      fieldInspectors.asJava
    )
  }
}
