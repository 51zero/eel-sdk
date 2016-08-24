import com.typesafe.sbt.pgp.PgpKeys
import sbt._
import sbt.Keys._

import scala.languageFeature.experimental
import scala.languageFeature.experimental.macros

object Build extends Build {

  val org = "io.eels"

  val ScalaVersion = "2.11.8"
  val ScalatestVersion = "3.0.0"
  val Slf4jVersion = "1.7.12"
  val Log4jVersion = "1.2.17"
  val HadoopVersion = "2.6.1"
  val HiveVersion = "1.1.0"

  val rootSettings = Seq(
    organization := org,
    scalaVersion := ScalaVersion,
    crossScalaVersions := Seq(ScalaVersion),
    publishMavenStyle := true,
    resolvers += Resolver.mavenLocal,
    publishArtifact in Test := false,
    parallelExecution in Test := false,
    scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8"),
    javacOptions := Seq("-source", "1.8", "-target", "1.8"),
    fork in test := true,
    javaOptions in test ++= Seq("-Xms256M", "-Xmx2G", "-XX:MaxPermSize=1024M", "-XX:+UseConcMarkSweepGC"),
    sbtrelease.ReleasePlugin.autoImport.releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    sbtrelease.ReleasePlugin.autoImport.releaseCrossBuild := true,
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "io.reactivex" %% "rxscala" % "0.26.2",
      "com.typesafe" % "config" % "1.3.0",
      "com.sksamuel.exts" %% "exts" % "1.31.1",
      "com.univocity" % "univocity-parsers" % "2.0.0",
      "com.h2database" % "h2" % "1.4.192",
      "org.apache.orc" % "orc-core" % "1.1.2",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.7.5",
      "org.apache.hadoop" % "hadoop-common" % HadoopVersion,
      "org.apache.hadoop" % "hadoop-hdfs" % HadoopVersion,
      "org.apache.hadoop" % "hadoop-mapreduce" % HadoopVersion,
      "org.apache.hadoop" % "hadoop-mapreduce-client" % HadoopVersion,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HadoopVersion,
      "org.apache.hadoop" % "hadoop-yarn" % HadoopVersion,
      "org.apache.hive" % "hive-exec" % HiveVersion exclude("org.pentaho", "pentaho-aggdesigner-algorithm") exclude("org.apache.calcite", "calcite-core") exclude("org.apache.calcite", "calcite-avatica"),
      "org.apache.parquet" % "parquet-avro" % "1.8.1",
      "org.slf4j" % "slf4j-api" % "1.7.21",
      "io.reactivex" % "rxjava" % "1.1.5",
      "io.dropwizard.metrics" % "metrics-core" % "3.1.2",
      "io.dropwizard.metrics" % "metrics-jvm" % "3.1.2",
      "mysql" % "mysql-connector-java" % "5.1.39" % "test",
      "org.scalatest" %% "scalatest" % "3.0.0" % "test"
    ),
    publishTo <<= version {
      (v: String) =>
        val nexus = "https://oss.sonatype.org/"
        if (v.trim.endsWith("SNAPSHOT"))
          Some("snapshots" at nexus + "content/repositories/snapshots")
        else
          Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    pomExtra := {
      <url>https://github.com/eel-sdk/eel</url>
        <licenses>
          <license>
            <name>MIT</name>
            <url>https://opensource.org/licenses/Apache2</url>
            <distribution>repo</distribution>
          </license>
        </licenses>
        <scm>
          <url>git@github.com:eel-sdk/eel.git</url>
          <connection>scm:git@github.com:eel-sdk/eel.git</connection>
        </scm>
        <developers>
          <developer>
            <id>sksamuel</id>
            <name>sksamuel</name>
            <url>http://github.com/sksamuel</url>
          </developer>
        </developers>
    }
  )

  lazy val root = Project("eel", file("."))
    .settings(rootSettings: _*)
    .settings(name := "eel")
    .aggregate(core)
   
  lazy val core = Project("eel-core", file("eel-core"))
    .settings(rootSettings: _*)
    .settings(name := "eel-core")
}
