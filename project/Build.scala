import com.typesafe.sbt.pgp.PgpKeys
import sbt._
import sbt.Keys._
import xerial.sbt.Pack._

object Build extends Build {


    libraryDependencies ++= Seq(
      "com.google.guava"            % "guava"                 % "18.0",
      "org.scalatest"               %% "scalatest"            % ScalatestVersion  % "test",
      "org.slf4j"                   % "slf4j-log4j12"         % Slf4jVersion      % "test",
      "log4j"                       % "log4j"                 % Log4jVersion      % "test"
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
            <url>https://opensource.org/licenses/MIT</url>
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
    .settings(publish := {})
    .settings(publishArtifact := false)
    .settings(name := "eel")
    .aggregate(core, cli)

  lazy val core = Project("eel-core", file("eel-core"))
    .settings(rootSettings: _*)
    .settings(libraryDependencies ++= Seq(

      "org.apache.hadoop"     % "hadoop-common"     % HadoopVersion,
      "org.apache.hadoop"     % "hadoop-client"     % HadoopVersion,
      "org.apache.hadoop"     % "hadoop-hdfs"       % HadoopVersion,
      "org.apache.hadoop"     % "hadoop-mapreduce"  % HadoopVersion,
      "org.apache.hadoop"     % "hadoop-mapreduce-client" % HadoopVersion,
      "org.apache.hive"       % "hive-common"       % HiveVersion,
      "com.fasterxml.jackson.core"      %  "jackson-databind"           % "2.7.2",
      "com.fasterxml.jackson.module"    %% "jackson-module-scala"       % "2.7.2",
      "mysql"                           % "mysql-connector-java"        % "5.1.38"
    ))
    .settings(name := "eel-core")

  lazy val cli = Project("eel-cli", file("eel-cli"))
    .settings(rootSettings: _*)
    .settings(packAutoSettings: _*)
    .settings(
      name := "eel-cli",
      packMain := Map("eel" -> "io.eels.cli.Main"),
      packExtraClasspath := Map("eel" -> Seq("${HADOOP_HOME}/etc/hadoop", "${HIVE_HOME}/conf")),
      packGenerateWindowsBatFile := true,
      packJarNameConvention := "default"
    )
    .settings(libraryDependencies ++= Seq(
      "com.github.scopt" %% "scopt" % "3.3.0"
    ))
    .dependsOn(core)
}
