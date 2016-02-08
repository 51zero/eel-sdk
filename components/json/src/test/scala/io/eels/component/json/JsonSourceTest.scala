package io.eels.component.json

import com.sksamuel.scalax.io.IO
import io.eels.{Column, Field, Row, SchemaType}
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class JsonSourceTest extends WordSpec with Matchers {

  "JsonSource" should {
    "read multiple json docs from a file" in {
      JsonSource(new Path(IO.fileFromResource("/test.json").getAbsolutePath)).toList.run shouldBe
        List(
          Row(
            List(Column("name", SchemaType.String, false), Column("location", SchemaType.String, false)),
            List(Field("sammy"), Field("aylesbury"))
          ),
          Row(
            List(Column("name", SchemaType.String, false), Column("location", SchemaType.String, false)),
            List(Field("ant"), Field("greece"))
          )
        )
    }
  }
}
