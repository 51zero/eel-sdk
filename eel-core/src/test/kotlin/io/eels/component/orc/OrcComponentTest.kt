package io.eels.component.orc

import io.eels.Frame
import io.eels.Row
import io.eels.schema.Field
import io.eels.schema.Schema
import io.kotlintest.specs.WordSpec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

class OrcComponentTest : WordSpec() {
  init {

    val fs = FileSystem.get(Configuration())

    "OrcComponent" should {
      "read and write orc files" {

        val schema = Schema(Field("name"), Field("job"), Field("location"))
        val frame = Frame(
            schema,
            Row(schema, listOf("clint eastwood", "actor", "carmel")),
            Row(schema, listOf("david bowie", "musician", "surrey"))
        )

        val path = Path("test.orc")
        frame.to(OrcSink(path))

        val rows = OrcSource(path, fs).toFrame(1).toSet()
        fs.delete(path, false)

        rows.first().schema shouldBe frame.schema()

        rows shouldBe setOf(
            Row(frame.schema(), listOf("clint eastwood", "actor", "carmel")),
            Row(frame.schema(), listOf("david bowie", "musician", "surrey"))
        )
      }
    }
  }
}