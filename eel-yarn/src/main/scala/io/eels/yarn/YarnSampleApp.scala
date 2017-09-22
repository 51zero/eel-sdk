package io.eels.yarn

import java.util
import javax.servlet.Servlet

import io.eels.component.parquet.ParquetSource
import org.apache.commons.cli.CommandLine
import org.apache.commons.codec.binary.BinaryCodec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Hdfs, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

object YarnSampleApp extends App {

  implicit val conf = new YarnConfiguration()
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/yarn-site.xml"))
  conf.reloadConfiguration()
  println(conf)

  implicit val fs = FileSystem.get(conf)

  val fn = ParquetSource("/home/sam").toDataStream().collect _

  val yarnClient = YarnClient.createYarnClient()
  yarnClient.init(conf)
  yarnClient.start()

  val app = yarnClient.createApplication()
  val appResponse = app.getNewApplicationResponse()

  println(appResponse)

  val appContext = app.getApplicationSubmissionContext()
  val appId = appContext.getApplicationId()

  println("App id = " + appResponse)

  appContext.setKeepContainersAcrossApplicationAttempts(false)
  appContext.setApplicationName("eel")

  val localResources = new java.util.HashMap[String, LocalResource]()
  val env = new java.util.HashMap[String, String]()

  val coreClasses = Seq(
    classOf[BinaryCodec], // commons-codec
    classOf[CommandLine], // commons-cli
    classOf[Servlet], // javax-servlet-api
    classOf[Hdfs], // hadoop-hdfs
    classOf[Configuration], // hadoop-common
    classOf[Product], // scala-sdk
    classOf[YarnClient], // hadoop-yarn-client
    classOf[YarnConfiguration], // hadoop-yarn-api
    classOf[com.google.common.base.Preconditions], // guava
    classOf[org.apache.commons.logging.Log], // commons-logging
    classOf[org.apache.commons.collections.ArrayStack], // commons-collections
    classOf[org.apache.commons.lang.ArrayUtils], // commons-lang
    classOf[org.apache.hadoop.yarn.ContainerLogAppender], // hadoop-yarn-common
    classOf[org.apache.commons.configuration.AbstractConfiguration], //commons-configuration
    classOf[org.apache.hadoop.security.authentication.client.Authenticator], //hadoop-auth
    classOf[org.slf4j.Logger], // slf4j-api
    classOf[org.apache.commons.io.Charsets], // commons-io
    classOf[com.google.protobuf.AbstractMessage] // protobuf-java
    // classOf[org.apache.htrace.Trace] // htrace-core
  )

  val ApplicationMasterClass = "io.eels.yarn.EelApplicationMaster"

  val classpath = (coreClasses.map(YarnUtils.jarForClass) ++ Seq("/home/sam/development/workspace/eel/eel-core/target/scala-2.11/classes")).mkString(":")

  val commands = Seq(
    "java",
    "-cp",
    classpath,
    ApplicationMasterClass,
    "1>",
    ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout",
    "2>",
    ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr"
  )

  val command = commands.mkString(" ")
  println(s"Application master will execute $command")

  val pri = Priority.newInstance(2)
  appContext.setPriority(pri)

  val resources = Resource.newInstance(1000, 1)
  appContext.setResource(resources)

  // Set up the container launch context for the application master
  val amContainer = ContainerLaunchContext.newInstance(localResources, env, util.Arrays.asList(command), null, null, null)
  appContext.setAMContainerSpec(amContainer)

  yarnClient.submitApplication(appContext)

  Thread.sleep(100000)
}


//  val credentials = new Credentials()

// For now, only getting tokens for the default file-system.
//  val tokens = fs.addDelegationTokens("sammy", credentials)
//  if (tokens != null) {
//    for (token <- tokens) {
//      println("Got dt for " + fs.getUri() + "; " + token)
//    }
//  }
//  val dob = new DataOutputBuffer()
//  credentials.writeTokenStorageToStream(dob)
//  val fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength())
// amContainerContext.setTokens(fsTokens)

//  val response = amRMClient.registerApplicationMaster("", 0, "")