package io.eels.cli

import io.eels.component.hive.HiveSink
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.scalatest.{Matchers, WordSpec}

class SinkFnTest extends WordSpec with Matchers {

  implicit val fs = FileSystem.get(new Configuration)
  implicit val conf = new HiveConf()

  "sink fn" should {
    "match hive with params" in {
      val sink = SinkFn("hive:db:tab?createTable=true")
      sink.asInstanceOf[HiveSink].createTable shouldBe true
    }
  }

}
