package io.eels.testkit

import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest.{BeforeAndAfterAll, Suite}

trait HiveTestKit extends BeforeAndAfterAll {
  this: Suite =>

  override protected def afterAll(): Unit = {
    fs.close()
    dir.toFile.delete()
  }

  val dir = Files.createTempDirectory("hive-test-kit")
  implicit val fs = FileSystem.getLocal(new Configuration)
  implicit val client = new InMemoryMetaStoreClient(dir.toAbsolutePath.toString, fs)
}
