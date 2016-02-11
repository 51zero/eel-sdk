import com.typesafe.sbt.pgp.PgpKeys
import sbt._
import sbt.Keys._

object Build extends Build {

  val org = "io.eels"

  val ScalaVersion = "2.11.7"
  val ScalatestVersion = "2.2.4"
  val Slf4jVersion = "1.7.12"
  val Log4jVersion = "1.2.17"
  val HadoopVersion = "2.6.1"
  val HiveVersion = "1.1.0"
  val Avro4sVersion = "1.2.2"

  val rootSettings = Seq(
    organization := org,
    scalaVersion := ScalaVersion,
    crossScalaVersions := Seq(ScalaVersion, "2.10.6"),
    publishMavenStyle := true,
    resolvers += Resolver.mavenLocal,
    resolvers += "conjars" at "http://conjars.org/repo/",
    publishArtifact in Test := false,
    parallelExecution in Test := false,
    scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8"),
    javacOptions := Seq("-source", "1.7", "-target", "1.7"),
    sbtrelease.ReleasePlugin.autoImport.releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    sbtrelease.ReleasePlugin.autoImport.releaseCrossBuild := true,
    libraryDependencies ++= Seq(
      "com.github.tototoshi"  %% "scala-csv"       % "1.2.2",
      "org.scala-lang"        % "scala-reflect"    % scalaVersion.value,
      "com.sksamuel.scalax"   %% "scalax"          % "0.14.0",
      "com.typesafe"          % "config"           % "1.2.1",
      "org.apache.hadoop"     % "hadoop-common"    % HadoopVersion % "provided",
      "org.apache.hadoop"     % "hadoop-hdfs"      % HadoopVersion % "provided",
      "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
      "com.google.guava"      % "guava"            % "18.0",
      "com.h2database"        % "h2"               % "1.4.191",
      "org.scalatest"         %% "scalatest"       % ScalatestVersion % "test",
      "org.slf4j"             % "slf4j-log4j12"    % Slf4jVersion % "test",
      "log4j"                 % "log4j"            % Log4jVersion % "test"
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
      <url>https://github.com/51zero/eel</url>
        <licenses>
          <license>
            <name>MIT</name>
            <url>https://opensource.org/licenses/MIT</url>
            <distribution>repo</distribution>
          </license>
        </licenses>
        <scm>
          <url>git@github.com:51zero/eel.git</url>
          <connection>scm:git@github.com:51zero/eel.git</connection>
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
    .settings(publish := {})
    .settings(publishArtifact := false)
    .settings(name := "eel")
    .aggregate(core, json, kafka, solr, parquet, avro, hive, orc)

  lazy val core = Project("eel-core", file("core"))
    .settings(rootSettings: _*)
    .settings(libraryDependencies ++= Seq(
      "io.dropwizard.metrics" %  "metrics-core"     % "3.1.2",
      "io.dropwizard.metrics" %  "metrics-jvm"      % "3.1.2"
    ))
    .settings(name := "eel-core")

  lazy val json = Project("eel-json", file("components/json"))
    .settings(rootSettings: _*)
    .settings(name := "eel-json")
    .settings(libraryDependencies ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.0"
    ))
    .dependsOn(core)

  lazy val hive = Project("eel-hive", file("components/hive"))
    .settings(rootSettings: _*)
    .settings(name := "eel-hive")
    .settings(libraryDependencies ++= Seq(
      "org.apache.hadoop"     % "hadoop-common"             % HadoopVersion % "provided",
      "org.apache.hadoop"     % "hadoop-client"             % HadoopVersion % "provided",
      "org.apache.hadoop"     % "hadoop-hdfs"               % HadoopVersion % "provided",
      "org.apache.hadoop"     % "hadoop-mapreduce"          % HadoopVersion % "provided",
      "org.apache.hadoop"     % "hadoop-mapreduce-client"   % HadoopVersion % "provided",
      "org.apache.hive"       % "hive-common"               % HiveVersion   % "provided",
      "org.apache.hive"       % "hive-exec"                 % HiveVersion   % "provided" exclude("org.pentaho", "pentaho-aggdesigner-algorithm"),
      "mysql" % "mysql-connector-java" % "5.1.38"
    ))
    .dependsOn(core, parquet)

  lazy val orc = Project("eel-orc", file("components/orc"))
    .settings(rootSettings: _*)
    .settings(name := "eel-orc")
    .settings(libraryDependencies ++= Seq(
      "org.apache.hadoop"     % "hadoop-common"             % HadoopVersion % "provided",
      "org.apache.hadoop"     % "hadoop-client"             % HadoopVersion % "provided",
      "org.apache.hadoop"     % "hadoop-hdfs"               % HadoopVersion % "provided",
      "org.apache.hadoop"     % "hadoop-mapreduce"          % HadoopVersion % "provided",
      "org.apache.hadoop"     % "hadoop-mapreduce-client"   % HadoopVersion % "provided",
      "org.apache.hive"       % "hive-exec"                 % HiveVersion   % "provided" exclude("org.pentaho", "pentaho-aggdesigner-algorithm")
    ))
    .dependsOn(core)

  lazy val kafka = Project("eel-kafka", file("components/kafka"))
    .settings(rootSettings: _*)
    .settings(name := "eel-kafka")
    .settings(libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "0.9.0.0"
    ))
    .dependsOn(core)

  lazy val avro = Project("eel-avro", file("components/avro"))
    .settings(rootSettings: _*)
    .settings(name := "eel-avro")
    .settings(libraryDependencies ++= Seq(
      "com.sksamuel.avro4s"   %% "avro4s-core"     % Avro4sVersion
    ))
    .dependsOn(core)

  lazy val parquet = Project("eel-parquet", file("components/parquet"))
    .settings(rootSettings: _*)
    .settings(name := "eel-parquet")
    .settings(libraryDependencies ++= Seq(
      "com.sksamuel.avro4s"   %% "avro4s-core"     % "1.2.2",
      "org.apache.parquet"    % "parquet-avro"     % "1.8.1"
    ))
    .dependsOn(core, avro)

  lazy val solr = Project("eel-solr", file("components/solr"))
    .settings(rootSettings: _*)
    .settings(name := "eel-solr")
    .settings(libraryDependencies ++= Seq(
      "org.apache.solr" % "solr-solrj" % "5.4.1"
    ))
    .dependsOn(core)

  lazy val elasticsearch = Project("eel-elasticsearch", file("components/elasticsearch"))
    .settings(rootSettings: _*)
    .settings(name := "eel-elasticsearch")
    .settings(libraryDependencies ++= Seq(
      "com.sksamuel.elastic4s" %% "elastic4s-core" % "2.1.1",
      "org.json4s"             %% "json4s-native"  % "3.3.0",
      "org.elasticsearch" % "elasticsearch" % "2.1.1" % "test"
    ))
    .dependsOn(core)
}
