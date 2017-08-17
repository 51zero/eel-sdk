package io.eels

import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, WordSpec}

class FilePatternTest extends WordSpec with Matchers {

  implicit val fs = FileSystem.get(new Configuration())

  "FilePattern" should {
    "detect single hdfs path without name server" ignore {
      FilePattern("hdfs:///mypath").toPaths() shouldBe List(new Path("hdfs:///mypath"))
    }
    "detect single hdfs path with name server" ignore {
      FilePattern("hdfs://nameserver/mypath").toPaths() shouldBe List(new Path("hdfs://nameserver/mypath"))
    }
    "detect absolute local file" in {
      FilePattern("file:///absolute/file").toPaths() shouldBe List(new Path("file:///absolute/file"))
    }
    "detect relative local file" in {
      FilePattern("file:///local/file").toPaths() shouldBe List(new Path("file:///local/file"))
    }
    "detect relative local file expansion" in {
      val dir = Files.createTempDirectory("filepatterntest")
      val files = List("a", "b", "c").map { it =>
        dir.resolve(it)
      }
      val hdfsPaths = files.map { it =>
        new Path("file://" + it)
      }
      files.foreach(file => Files.createFile(file))
      FilePattern("file://" + dir.toAbsolutePath().toString() + "/*").toPaths().toSet shouldBe hdfsPaths.toSet
      files.foreach(Files.deleteIfExists)
      Files.deleteIfExists(dir)
    }

    //not working on windows
    "detect relative local file expansion with schema" in {
      val dir = Files.createTempDirectory("filepatterntest")
      val files = List("a", "b", "c").map { it =>
        dir.resolve(it)
      }
      val hdfsPaths = files.map { it =>
        new Path("file://" + it)
      }
      files.foreach(file => Files.createFile(file))
      FilePattern(dir.toAbsolutePath().toString() + "/*").toPaths().toSet shouldBe hdfsPaths.toSet
      files.foreach(Files.deleteIfExists)
      Files.deleteIfExists(dir)
    }

    "use filter if supplied" in {
      val dir = Files.createTempDirectory("filepatterntest")
      val files = List("a", "b", "c").map { it => dir.resolve(it) }
      files.foreach { it => Files.createFile(it) }
      val a = FilePattern(dir.toAbsolutePath().toString() + "/*")
        .withFilter(_.toString().endsWith("a"))
        .toPaths.toSet
      a shouldBe Set(new Path("file:///" + dir.resolve("a")))
      files.foreach { it => Files.deleteIfExists(it) }
      Files.deleteIfExists(dir)
    }
  }
}