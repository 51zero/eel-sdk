package io.eels.source

import com.sksamuel.scalax.io.IO
import io.eels.{SchemaType, Column, Row, Field}
import org.apache.hadoop.fs.Path
import org.scalatest.{WordSpec, Matchers}

class JsonSourceTest extends WordSpec with Matchers {

  "JsonSource" should {
    "read multiple json docs from a file" in {
      JsonSource(new Path(IO.fileFromResource("/test.json").getAbsolutePath)).toList shouldBe
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
