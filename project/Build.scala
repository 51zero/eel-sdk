import com.typesafe.sbt.pgp.PgpKeys
import sbt._
import sbt.Keys._

import scala.languageFeature.experimental
import scala.languageFeature.experimental.macros

object Build extends Build {

  val org = "io.eels"

  val AvroVersion = "1.8.1"
  val DerbyVersion = "10.13.1.1"
  val ExtsVersion = "1.44.1"
  val H2Version = "1.4.192"
  val HadoopVersion = "2.6.4"
  val HiveVersion = "1.2.1"
  val JacksonVersion = "2.8.6"
  val Log4jVersion = "1.2.17"
  val MetricsVersion = "3.1.2"
  val MysqlVersion = "5.1.39"
  val OrcVersion = "1.3.0"
  val ParquetVersion = "1.9.0"
  val ScalaVersion = "2.11.8"
  val ScalatestVersion = "3.0.0"
  val Slf4jVersion = "1.7.12"
  val UnivocityVersion = "2.2.3"
  val ConfigVersion = "1.3.0"
  val KafkaVersion = "0.10.1.1"
  val KuduVersion = "1.1.0"

  val hiveSettings = Seq(
    libraryDependencies ++= Seq(
      "org.apache.hadoop"   % "hadoop-yarn"               % HadoopVersion,
      "org.apache.hive"     % "hive-exec"                 % HiveVersion exclude("org.pentaho", "pentaho-aggdesigner-algorithm") exclude("org.apache.calcite", "calcite-core") exclude("org.apache.calcite", "calcite-avatica") exclude("org.apache.logging.log4j", "log4j-slf4j-impl"),
      "org.apache.logging.log4j" % "log4j-api"            % "2.7"     % "test",
      "org.apache.logging.log4j" % "log4j-core"           % "2.7"     % "test",
      "org.apache.logging.log4j" % "log4j-slf4j-impl"     % "2.7"     % "test"
    )
  )

  val componentsSettings = Seq(
    libraryDependencies ++= Seq(
      "org.apache.hadoop"                       % "hadoop-hdfs"                         % HadoopVersion,
      "org.apache.hadoop"                       % "hadoop-mapreduce"                    % HadoopVersion,
      "org.apache.hadoop"                       % "hadoop-mapreduce-client"             % HadoopVersion,
      "org.apache.hadoop"                       % "hadoop-mapreduce-client-core"        % HadoopVersion,
      "org.apache.hadoop"                       % "hadoop-yarn-client"                  % HadoopVersion,
      "org.apache.hadoop"                       % "hadoop-yarn-server-resourcemanager"  % HadoopVersion,
      "org.apache.parquet"                      % "parquet-avro"                        % ParquetVersion,
      "org.apache.derby"                        % "derby"                               % DerbyVersion,
      "com.h2database"                          % "h2"                                  % H2Version,
      "org.apache.spark"                        %% "spark-sql"                          % "2.1.0"           % "test"
    )
  )

  val orcSettings = Seq(
    libraryDependencies ++= Seq(
      "org.apache.orc"                          % "orc-core"                % OrcVersion,
      "org.apache.orc"                          % "orc-mapreduce"           % OrcVersion
    )
  )

  val kafkaSettings = Seq(
    libraryDependencies ++= Seq(
      "org.apache.kafka"            %  "kafka-clients"                  % KafkaVersion,
      "net.manub"                   %% "scalatest-embedded-kafka"       % "0.11.0"     % "test"
    )
  )

  val kuduSettings = Seq(
    libraryDependencies ++= Seq(
      "org.apache.kudu" % "kudu-client" % KuduVersion
    )
  )

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
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-l", "kudu"),
    libraryDependencies ++= Seq(
      "org.scala-lang"              % "scala-reflect"           % scalaVersion.value,
      "com.typesafe"                % "config"                  % ConfigVersion,
      "com.sksamuel.exts"           %% "exts"                   % ExtsVersion,
      "com.univocity"               % "univocity-parsers"       % UnivocityVersion,
      "org.apache.avro"             % "avro"                    % AvroVersion,
      "org.apache.hadoop"           % "hadoop-common"           % HadoopVersion exclude("org.slf4j","slf4j-log4j12"),
      "io.dropwizard.metrics"       % "metrics-core"            % MetricsVersion,
      "io.dropwizard.metrics"       % "metrics-jvm"             % MetricsVersion,
      "org.slf4j"                   % "slf4j-api"               % Slf4jVersion,
      "com.fasterxml.jackson.module"%% "jackson-module-scala"   % JacksonVersion,
      "org.apache.logging.log4j"    % "log4j-api"               % "2.7"             % "test",
      "org.apache.logging.log4j"    % "log4j-core"              % "2.7"             % "test",
      "org.apache.logging.log4j"    % "log4j-slf4j-impl"        % "2.7"             % "test",
      "mysql"                       % "mysql-connector-java"    % MysqlVersion      % "test",
      "org.scalatest"               %% "scalatest"              % ScalatestVersion  % "test"
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
    .aggregate(core, components, orc, hive, kafka, kudu)
   
  lazy val core = Project("eel-core", file("eel-core"))
    .settings(rootSettings: _*)
    .settings(name := "eel-core")

  lazy val components = Project("eel-components", file("eel-components"))
    .settings(rootSettings: _*)
    .settings(componentsSettings: _*)
    .settings(name := "eel-components")
    .dependsOn(core)

  lazy val orc = Project("eel-orc", file("eel-orc"))
    .settings(rootSettings: _*)
    .settings(componentsSettings: _*)
    .settings(orcSettings: _*)
    .settings(name := "eel-orc")
    .dependsOn(core)

  lazy val hive = Project("eel-hive", file("eel-hive"))
    .settings(rootSettings: _*)
    .settings(componentsSettings: _*)
    .settings(hiveSettings: _*)
    .settings(name := "eel-hive")
    .dependsOn(core, components, orc)

  lazy val kafka = Project("eel-kafka", file("eel-kafka"))
    .settings(rootSettings: _*)
    .settings(componentsSettings: _*)
    .settings(kafkaSettings: _*)
    .settings(name := "eel-kafka")
    .dependsOn(core)

  lazy val kudu = Project("eel-kudu", file("eel-kudu"))
    .settings(rootSettings: _*)
    .settings(componentsSettings: _*)
    .settings(kuduSettings: _*)
    .settings(name := "eel-kudu")
    .dependsOn(core)
}
