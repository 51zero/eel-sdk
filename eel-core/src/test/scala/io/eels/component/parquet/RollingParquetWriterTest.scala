package io.eels.component.parquet

import com.typesafe.config.ConfigFactory
import io.eels.Frame
import io.eels.component.avro.{AvroRecordFn, AvroSchemaFn}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpec}

class RollingParquetWriterTest extends WordSpec with Matchers with BeforeAndAfterEach {

  ParquetLogMute()

  import scala.concurrent.ExecutionContext.Implicits.global

  val frame = Frame(
    List("name", "job", "location"),
    List("clint eastwood", "actor", "carmel"),
    List("elton john", "musician", "pinner"),
    List("david bowie", "musician", "london"),
    List("jack bruce", "musician", "glasgow")
  )

  implicit val fs = FileSystem.get(new Configuration)
  val basePath = new Path("parquet")
  val path0 = new Path("parquet_0")
  val path1 = new Path("parquet_1")

  def cleanup(): Unit = {
    if (fs.exists(basePath))
      fs.delete(basePath, true)
    if (fs.exists(path0))
      fs.delete(path0, true)
    if (fs.exists(path1))
      fs.delete(path1, true)
  }

  val avroSchema = AvroSchemaFn.toAvro(frame.schema)

  "RollingParquetWriter" should {
    "rollover on record count" in {
      val writer = new RollingParquetWriter(basePath, avroSchema, 2, 0, true)
      frame.buffer.iterator.toList.foreach(row => writer.write(AvroRecordFn.toRecord(row, avroSchema, frame.schema, ConfigFactory.load)))
      writer.close()
      ParquetSource(path0).toSet.map(_.values.map(_.toString)) shouldBe
        Set(
          List("clint eastwood", "actor", "carmel"),
          List("elton john", "musician", "pinner")
        )
      ParquetSource(path1).toSet.map(_.values.map(_.toString)) shouldBe
        Set(
          List("david bowie", "musician", "london"),
          List("jack bruce", "musician", "glasgow")
        )
    }
    "write crc" in {
      val writer = new RollingParquetWriter(basePath, avroSchema, 2, 0, false)
      frame.buffer.iterator.toList.foreach(row => writer.write(AvroRecordFn.toRecord(row, avroSchema, frame.schema, ConfigFactory.load)))
      writer.close()
      fs.exists(new Path(".parquet_0.crc")) shouldBe true
    }
    "skip crc if option set" in {
      val writer = new RollingParquetWriter(basePath, avroSchema, 2, 0, true)
      frame.buffer.iterator.toList.foreach(row => writer.write(AvroRecordFn.toRecord(row, avroSchema, frame.schema, ConfigFactory.load)))
      writer.close()
      fs.exists(new Path(".parquet_0.crc")) shouldBe false
    }
  }


  override protected def afterEach(): Unit = cleanup()
  override protected def beforeEach(): Unit = cleanup()
}

