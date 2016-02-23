package io.eels.component.json

import com.sksamuel.scalax.io.IO
import io.eels.{FrameSchema, Row}
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class JsonSourceTest extends WordSpec with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  "JsonSource" should {
    "read multiple json docs from a file" in {
      val expectedSchema = FrameSchema("name", "location")
      JsonSource(new Path(IO.fileFromResource("/test.json").getAbsolutePath)).toSet shouldBe
        Set(
          Row(expectedSchema, "sammy", "aylesbury"),
          Row(expectedSchema, "ant", "greece")
        )
    }
  }
}
