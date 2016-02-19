package io.eels.component.json

import com.sksamuel.scalax.io.IO
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class JsonSourceTest extends WordSpec with Matchers {

  "JsonSource" should {
    "read multiple json docs from a file" in {
      JsonSource(new Path(IO.fileFromResource("/test.json").getAbsolutePath)).toSeq.run shouldBe
        List(
          List("sammy", "aylesbury"),
          List("ant", "greece")
        )
    }
  }
}
