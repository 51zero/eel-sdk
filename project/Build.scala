import com.typesafe.sbt.pgp.PgpKeys
import sbt._
import sbt.Keys._

import scala.languageFeature.experimental
import scala.languageFeature.experimental.macros

object Build extends Build {

  val org = "io.eels"

  val ExtsVersion = "1.37.0"
  val HadoopVersion = "2.7.3"
  val HiveVersion = "2.1.0"
  val JacksonVersion = "2.8.4"
  val Log4jVersion = "1.2.17"
  val OrcVersion = "1.2.1"
  val ScalaVersion = "2.11.8"
  val ScalatestVersion = "3.0.0"
  val Slf4jVersion = "1.7.12"
  val ParquetVersion = "1.8.1"
  val UnivocityVersion = "2.2.3"
  val RxJavaVersion = "2.0.1"
  val ConfigVersion = "1.3.0"
  val H2Version = "1.4.192"
  val MetricsVersion = "3.1.2"

  val hiveSettings = Seq(
    libraryDependencies ++= Seq(
      "org.apache.hadoop"   % "hadoop-yarn"                           % HadoopVersion,
      "org.apache.hive"     % "hive-exec"                             % HiveVersion exclude("org.pentaho", "pentaho-aggdesigner-algorithm") exclude("org.apache.calcite", "calcite-core") exclude("org.apache.calcite", "calcite-avatica") exclude("org.apache.logging.log4j", "log4j-slf4j-impl"),
      "org.datanucleus"     % "datanucleus-core"                      % "4.1.15",
      "org.datanucleus"     % "datanucleus-api-jdo"                   % "4.1.4",
      "org.datanucleus"     % "datanucleus-accessplatform-jdo-rdbms"  % "4.1.15",
      "com.jolbox"          % "bonecp"                                % "0.8.0.RELEASE",
      "org.apache.logging.log4j" % "log4j-api"            % "2.7"             % "test",
      "org.apache.logging.log4j" % "log4j-core"           % "2.7"             % "test",
      "org.apache.logging.log4j" % "log4j-slf4j-impl"     % "2.7"             % "test"
    )
  )

  val componentsSettings = Seq(
    libraryDependencies ++= Seq(
      "org.apache.orc"                          % "orc-core"                % OrcVersion,
      "org.apache.orc"                          % "orc-mapreduce"           % OrcVersion,
      "com.fasterxml.jackson.module"            %% "jackson-module-scala"   % JacksonVersion,
      "org.apache.hadoop"                       % "hadoop-hdfs"             % HadoopVersion,
      "org.apache.hadoop"                       % "hadoop-mapreduce"        % HadoopVersion,
      "org.apache.hadoop"                       % "hadoop-mapreduce-client" % HadoopVersion,
      "org.apache.hadoop"                       % "hadoop-mapreduce-client-core" % HadoopVersion,
      "org.apache.parquet"                      % "parquet-avro"            % ParquetVersion,
      "org.apache.derby"                        % "derby"                   % "10.13.1.1",
      "com.h2database"                          % "h2"                      % H2Version
    )
  )

  val rootSettings = Seq(
    organization := org,
    scalaVersion := ScalaVersion,
    crossScalaVersions := Seq(ScalaVersion, "2.11.8"),
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
      "org.scala-lang"        % "scala-reflect"           % scalaVersion.value,
      "io.projectreactor"     % "reactor-core"            % "3.0.3.RELEASE",
      "com.typesafe"          % "config"                  % ConfigVersion,
      "com.sksamuel.exts"     %% "exts"                   % ExtsVersion,
      "com.univocity"         % "univocity-parsers"       % UnivocityVersion,
      "org.apache.avro" % "avro" % "1.8.1",
      "org.apache.hadoop"     % "hadoop-common"           % HadoopVersion exclude("org.slf4j","slf4j-log4j12"),
      "io.dropwizard.metrics" % "metrics-core"            % MetricsVersion,
      "io.dropwizard.metrics" % "metrics-jvm"             % MetricsVersion,
      "org.slf4j"             % "slf4j-api"               % "1.7.21",
      "org.apache.logging.log4j" % "log4j-api"            % "2.7"             % "test",
      "org.apache.logging.log4j" % "log4j-core"           % "2.7"             % "test",
      "org.apache.logging.log4j" % "log4j-slf4j-impl"     % "2.7"             % "test",
      "mysql"                 % "mysql-connector-java"    % "5.1.39"          % "test",
      "org.scalatest"         %% "scalatest"              % ScalatestVersion  % "test"
    ),
    excludeDependencies += "org.slf4j" % "slf4j-log4j12",
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
    .aggregate(core, components, hive)
   
  lazy val core = Project("eel-core", file("eel-core"))
    .settings(rootSettings: _*)
    .settings(name := "eel-core")

  lazy val components = Project("eel-components", file("eel-components"))
    .settings(rootSettings: _*)
    .settings(componentsSettings: _*)
    .settings(name := "eel-components")
    .dependsOn(core)

  lazy val hive = Project("eel-hive", file("eel-hive"))
    .settings(rootSettings: _*)
    .settings(componentsSettings: _*)
    .settings(hiveSettings: _*)
    .settings(name := "eel-hive")
    .dependsOn(core, components)
}
