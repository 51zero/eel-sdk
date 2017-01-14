package io.eels.component.kudu

import io.eels.Frame
import io.eels.schema.{Field, StringType, StructType}
import org.scalatest.{Matchers, WordSpec}

class KuduSinkTest extends WordSpec with Matchers {

  val schema = StructType(
    Field("planet", StringType, nullable = false, key = true),
    Field("position", StringType, nullable = true)
  )

  val frame = Frame.fromValues(
    schema,
    Vector("earth", 3),
    Vector("saturn", 6)
  )

  val master = "localhost:7051"
  frame.to(KuduSink(master, "mytable"))
}
