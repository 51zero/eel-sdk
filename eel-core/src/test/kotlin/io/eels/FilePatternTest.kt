package io.eels

import java.nio.file.Files

import io.kotlintest.specs.WordSpec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

class FilePatternTest : WordSpec() {

  val fs = FileSystem.get(Configuration())

  init {
    "FilePattern" should {
      "detect single hdfs path without name server" with {
        FilePattern("hdfs:///mypath", fs).toPaths() shouldBe listOf(Path("hdfs:///mypath"))
      }
      "detect single hdfs path with name server" with {
        FilePattern("hdfs://nameserver/mypath", fs).toPaths() shouldBe listOf(Path("hdfs://nameserver/mypath"))
      }
      "detect absolute local file" with {
        FilePattern("file:///absolute/file", fs).toPaths() shouldBe listOf(Path("file:///absolute/file"))
      }
      "detect relative local file" with {
        FilePattern("file://local/file", fs).toPaths() shouldBe listOf(Path("file://local/file"))
      }
      "detect relative local file expansion" with {
        val dir = Files.createTempDirectory("filepatterntest")
        val files = listOf("a", "b", "c").map { dir.resolve(it) }
        val hdfsPaths = files.map { Path("file://" + it) }
        files.forEach { Files.createFile(it) }
        FilePattern("file://" + dir.toAbsolutePath().toString() + "/*", fs).toPaths().toSet() shouldBe hdfsPaths.toSet()
        files.forEach { Files.deleteIfExists(it) }
        Files.deleteIfExists(dir)
      }

      //not working on windows
      "detect relative local file expansion with schema" with  {
        val dir = Files.createTempDirectory("filepatterntest")
        val files = listOf("a", "b", "c").map { dir.resolve(it) }
        val hdfsPaths = files.map { Path("file://" + it) }
        files.forEach { Files.createFile(it) }
        FilePattern(dir.toAbsolutePath().toString() + "/*", fs).toPaths().toSet() shouldBe hdfsPaths.toSet()
        files.forEach { Files.deleteIfExists(it) }
        Files.deleteIfExists(dir)
      }

      "use filter if supplied" with {
        val dir = Files.createTempDirectory("filepatterntest")
        val files = listOf("a", "b", "c").map { dir.resolve(it) }
        files.forEach { Files.createFile(it) }
        FilePattern(dir.toAbsolutePath().toString() + "/*", fs)
            .withFilter { it.toString().endsWith("a") }.toPaths().toSet() shouldBe
            setOf(Path("file:///" + dir.resolve("a")))
        files.forEach { Files.deleteIfExists(it) }
        Files.deleteIfExists(dir)
      }
    }
  }
}