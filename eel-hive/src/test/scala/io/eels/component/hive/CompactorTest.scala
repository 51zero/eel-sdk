package io.eels.component.hive

import java.io.File

import io.eels.datastream.DataStream
import io.eels.schema.{Field, StructType}

class CompactorTest extends HiveTests {

  import HiveConfig._

  HiveTable("default", "wibble").drop(true)

  val schema = StructType(Field("a"), Field("b"))
  val ds = DataStream.fromValues(schema, Seq(
    Array("1", "2"),
    Array("3", "4"),
    Array("5", "6"),
    Array("7", "8")
  ))
  ds.to(HiveSink("default", "wibble").withCreateTable(true))

  "Compactor" should {
    "delete the originals" in {
      assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)

      HiveTable("default", "wibble").paths(false, false).size should be > 1
      Compactor("default", "wibble").compactTo(1)
      HiveTable("default", "wibble").paths(false, false).size should be
      1
    }
    "merge the contents" in {
      assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)

      HiveSource("default", "wibble").toDataStream().collectValues shouldBe Seq(
        Array("1", "2"),
        Array("3", "4"),
        Array("5", "6"),
        Array("7", "8")
      )
    }
  }
}