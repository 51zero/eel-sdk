package io.eels

import java.nio.file.Files

import org.apache.hadoop.fs.Path
import org.scalatest.{WordSpec, Matchers}

class FilePatternTest extends WordSpec with Matchers {

  "FilePattern" should {
    "detect single hdfs path without name server" in {
      FilePattern("hdfs:///mypath").toPaths shouldBe Seq(new Path("hdfs:///mypath"))
    }
    "detect single hdfs path with name server" in {
      FilePattern("hdfs://nameserver/mypath").toPaths shouldBe Seq(new Path("hdfs://nameserver/mypath"))
    }
    "detect absolute local file" in {
      FilePattern("file:///absolute/file").toPaths shouldBe Seq(new Path("file:///absolute/file"))
    }
    "detect relative local file" in {
      FilePattern("file://local/file").toPaths shouldBe Seq(new Path("file://local/file"))
    }
    "detect relative local file expansion" in {
      val dir = Files.createTempDirectory("filepatterntest")
      val files = List("a", "b", "c").map(dir.resolve)
      val hdfsPaths = files.map(path => new Path("file:" + path))
      files.foreach(Files.createFile(_))
      FilePattern("file://" + dir.toAbsolutePath.toString + "/*").toPaths.toSet shouldBe hdfsPaths.toSet
      files.foreach(Files.deleteIfExists)
      Files.deleteIfExists(dir)
    }
    "detect relative local file expansion with schema" in {
      val dir = Files.createTempDirectory("filepatterntest")
      val files = List("a", "b", "c").map(dir.resolve)
      val hdfsPaths = files.map(path => new Path("file:" + path))
      files.foreach(Files.createFile(_))
      FilePattern(dir.toAbsolutePath.toString + "/*").toPaths.toSet shouldBe hdfsPaths.toSet
      files.foreach(Files.deleteIfExists)
      Files.deleteIfExists(dir)
    }
  }
}
