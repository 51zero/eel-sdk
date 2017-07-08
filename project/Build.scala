import com.typesafe.sbt.pgp.PgpKeys
import sbt._
import sbt.Keys._

import scala.languageFeature.experimental
import scala.languageFeature.experimental.macros

object Build extends Build {

  val org = "io.eels"

  val AvroVersion = "1.8.2"
  val ConfigVersion = "1.3.0"
  val Elastic4sVersion = "5.4.6"
  val ExtsVersion = "1.49.0"
  val H2Version = "1.4.196"
  val HadoopVersion = "2.6.5"
  val HiveVersion = "1.2.2"
  val JacksonVersion = "2.8.9"
  val KafkaVersion = "0.10.2.1"
  val KuduVersion = "1.4.0"
  val Log4jVersion = "2.7"
  val MetricsVersion = "3.1.2"
  val MysqlVersion = "5.1.42"
  val OrcVersion = "1.4.0"
  val ParquetVersion = "1.9.0"
  val RxJavaVersion = "2.1.1"
  val ScalatestVersion = "3.0.3"
  val Slf4jVersion = "1.7.12"
  val SparkVersion = "2.1.1"
  val UnivocityVersion = "2.4.1"

  val hiveSettings = Seq(
    libraryDependencies ++= Seq(
      "org.apache.hadoop"   % "hadoop-yarn"               % HadoopVersion,
      "org.apache.hive"     % "hive-exec"                 % HiveVersion exclude("org.pentaho", "pentaho-aggdesigner-algorithm") exclude("org.apache.calcite", "calcite-core") exclude("org.apache.calcite", "calcite-avatica") exclude("org.apache.logging.log4j", "log4j-slf4j-impl"),
      "org.apache.logging.log4j" % "log4j-api"            % Log4jVersion     % "test",
      "org.apache.logging.log4j" % "log4j-core"           % Log4jVersion     % "test",
      "org.apache.logging.log4j" % "log4j-slf4j-impl"     % Log4jVersion     % "test"
    )
  )

  val orcSettings = Seq(
    libraryDependencies ++= Seq(
      "org.apache.orc"                          % "orc-core"                % OrcVersion
    )
  )

  val kafkaSettings = Seq(
    libraryDependencies ++= Seq(
      "org.apache.kafka"            %  "kafka-clients"                  % KafkaVersion,
      "net.manub"                   %% "scalatest-embedded-kafka"       % "0.13.1"     % "test"
    )
  )

  val kuduSettings = Seq(
    libraryDependencies ++= Seq(
      "org.apache.kudu" % "kudu-client" % KuduVersion
    )
  )

  val esSettings = Seq(
    libraryDependencies ++= Seq(
      "com.sksamuel.elastic4s" %% "elastic4s-http" % Elastic4sVersion
    )
  )

  val rootSettings = Seq(
    organization := org,
    scalaVersion := "2.11.11",
    crossScalaVersions := Seq("2.11.11"),
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
      "org.scala-lang"              % "scala-reflect"                       % scalaVersion.value,
      "com.typesafe"                % "config"                              % ConfigVersion,
      "com.sksamuel.exts"           %% "exts"                               % ExtsVersion,
      "com.univocity"               % "univocity-parsers"                   % UnivocityVersion,
      "org.apache.avro"             % "avro"                                % AvroVersion,
      "org.apache.hadoop"           % "hadoop-common"                       % HadoopVersion exclude("org.slf4j","slf4j-log4j12"),
      "org.apache.hadoop"           % "hadoop-hdfs"                         % HadoopVersion,
      "org.apache.hadoop"           % "hadoop-mapreduce"                    % HadoopVersion,
      "org.apache.hadoop"           % "hadoop-mapreduce-client"             % HadoopVersion,
      "org.apache.hadoop"           % "hadoop-mapreduce-client-core"        % HadoopVersion,
      "org.apache.hadoop"           % "hadoop-yarn-client"                  % HadoopVersion,
      "org.apache.hadoop"           % "hadoop-yarn-server-resourcemanager"  % HadoopVersion,
      "org.apache.parquet"          % "parquet-avro"                        % ParquetVersion,
      "com.h2database"              % "h2"                                  % H2Version,
      "io.dropwizard.metrics"       % "metrics-core"            % MetricsVersion,
      "io.dropwizard.metrics"       % "metrics-jvm"             % MetricsVersion,
      "org.slf4j"                   % "slf4j-api"               % Slf4jVersion,
      "com.fasterxml.jackson.module"%% "jackson-module-scala"   % JacksonVersion,
      "org.apache.spark"            %% "spark-sql"              % SparkVersion             % "test",
      "org.apache.logging.log4j"    % "log4j-api"               % Log4jVersion             % "test",
      "org.apache.logging.log4j"    % "log4j-core"              % Log4jVersion             % "test",
      "org.apache.logging.log4j"    % "log4j-slf4j-impl"        % Log4jVersion             % "test",
      "mysql"                       % "mysql-connector-java"    % MysqlVersion             % "test",
      "org.scalatest"               %% "scalatest"              % ScalatestVersion         % "test"
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
    .aggregate(core,
      orc,
      hive,
      kafka,
      kudu,
      elasticsearch)

  lazy val core = Project("eel-core", file("eel-core"))
    .settings(rootSettings: _*)
    .settings(name := "eel-core")

  lazy val orc = Project("eel-orc", file("eel-orc"))
    .settings(rootSettings: _*)
    .settings(orcSettings: _*)
    .settings(name := "eel-orc")
    .dependsOn(core)

  lazy val hive = Project("eel-hive", file("eel-hive"))
    .settings(rootSettings: _*)
    .settings(hiveSettings: _*)
    .settings(name := "eel-hive")
    .dependsOn(core, orc)

  lazy val kafka = Project("eel-kafka", file("eel-kafka"))
    .settings(rootSettings: _*)
    .settings(kafkaSettings: _*)
    .settings(name := "eel-kafka")
    .dependsOn(core)

  lazy val kudu = Project("eel-kudu", file("eel-kudu"))
    .settings(rootSettings: _*)
    .settings(kuduSettings: _*)
    .settings(name := "eel-kudu")
    .dependsOn(core)

  lazy val elasticsearch = Project("eel-elasticsearch", file("eel-elasticsearch"))
    .settings(rootSettings: _*)
    .settings(esSettings: _*)
    .settings(name := "eel-elasticsearch")
    .dependsOn(core)
}
