package io.eels.component.hive

import io.eels.testkit.HiveTestKit
import io.eels.{Column, Frame, Schema}
import org.apache.commons.io.IOUtils
import org.scalatest.{Matchers, WordSpec}

class HiveSinkTest extends WordSpec with Matchers with HiveTestKit {

  import scala.concurrent.ExecutionContext.Implicits.global

  val schema = Schema(Column("a"), Column("p"))
  HiveOps.createTable("sam", "sinktest", schema, format = HiveFormat.Parquet, partitionKeys = List("p"))

  Frame(schema, Seq.fill(100000)(Seq("HOLLY", "QUEEG"))).to(HiveSink("sam", "sinktest").withIOThreads(2))

  "HiveSink" should {
    "not write partition data to the data file" in {
      val table = client.getTable("sam", "sinktest")
      val paths = HiveFilesFn(table, Nil).map(_._1).map(_.getPath)
      for (path <- paths) {
        val str = IOUtils.toString(fs.open(path))
        // none of these files should not contain the partition data QUEEG
        // but should all contain HOLLY
        str should include("HOLLY")
        str should not include "QUEEG"
      }
    }
  }
}

