import com.typesafe.sbt.pgp.PgpKeys
import sbt._
import sbt.Keys._

object Build extends Build {

  val org = "com.sksamuel.hadoop-streams"

  val ScalaVersion = "2.11.7"
  val ScalatestVersion = "3.0.0-M12"
  val Slf4jVersion = "1.7.12"
  val Log4jVersion = "1.2.17"

  val rootSettings = Seq(
    organization := org,
    scalaVersion := ScalaVersion,
    crossScalaVersions := Seq(ScalaVersion, "2.10.6"),
    publishMavenStyle := true,
    resolvers += Resolver.mavenLocal,
    publishArtifact in Test := false,
    parallelExecution in Test := false,
    scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8"),
    javacOptions := Seq("-source", "1.7", "-target", "1.7"),
    sbtrelease.ReleasePlugin.autoImport.releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    sbtrelease.ReleasePlugin.autoImport.releaseCrossBuild := true,
    libraryDependencies ++= Seq(
      "org.slf4j"             % "slf4j-api"        % Slf4jVersion,
      "org.slf4j"             % "log4j-over-slf4j" % Slf4jVersion,
      "log4j"                 % "log4j"            % Log4jVersion,
      "com.github.tototoshi"  %% "scala-csv"       % "1.2.2",
      "com.typesafe"          % "config"           % "1.3.0",
      "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
      "org.apache.hadoop"     % "hadoop-common"    % "2.7.1",
      "org.apache.hadoop"     % "hadoop-hdfs"      % "2.7.1",
      "org.apache.parquet"    % "parquet-avro"     % "1.8.1",
      "com.sksamuel.avro4s"   %% "avro4s-core"     % "1.2.2" % "test",
      "org.scalatest"         %% "scalatest"       % ScalatestVersion % "test",
      "com.h2database" % "h2" % "1.4.191" % "test"
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
      <url>https://github.com/sksamuel/hadoop-streams</url>
        <licenses>
          <license>
            <name>MIT</name>
            <url>https://opensource.org/licenses/MIT</url>
            <distribution>repo</distribution>
          </license>
        </licenses>
        <scm>
          <url>git@github.com:sksamuel/hadoop-streams.git</url>
          <connection>scm:git@github.com:sksamuel/hadoop-streams.git</connection>
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

  lazy val root = Project("hadoop-streams", file("."))
    .settings(rootSettings: _*)
    .settings(publish := {})
    .settings(publishArtifact := false)
    .settings(name := "hadoop-streams")
    .aggregate(core)

  lazy val core = Project("hadoop-streams-core", file("core"))
    .settings(rootSettings: _*)
    .settings(publish := {})
    .settings(name := "hadoop-streams-core")
}
