package io.eels.yarn

import java.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

object YarnSampleApp extends App {

  val conf = new YarnConfiguration()
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/yarn-site.xml"))
  conf.reloadConfiguration()
  println(conf)

  implicit val fs = FileSystem.get(conf)

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
  val commands = util.Arrays.asList("java -cp /home/sam/.ivy2/cache/commons-codec/commons-codec/jars/commons-codec-1.10.jar:/home/sam/.ivy2/cache/commons-cli/commons-cli/jars/commons-cli-1.3.1.jar:/home/sam/.ivy2/cache/javax.servlet/javax.servlet-api/jars/javax.servlet-api-3.1.0.jar:/home/sam/.ivy2/cache/org.apache.hadoop/hadoop-hdfs/jars/hadoop-hdfs-2.7.2.jar:/home/sam/.gradle/caches/modules-2/files-2.1/org.scala-lang/scala-library/2.11.8/ddd5a8bced249bedd86fb4578a39b9fb71480573/scala-library-2.11.8.jar:/home/sam/.ivy2/cache/org.apache.hadoop/hadoop-yarn-client/jars/hadoop-yarn-client-2.7.2.jar:/home/sam/.ivy2/cache/org.apache.hadoop/hadoop-yarn-api/jars/hadoop-yarn-api-2.7.2.jar:/home/sam/.ivy2/cache/org.apache.hadoop/hadoop-common/jars/hadoop-common-2.7.2.jar:/home/sam/.ivy2/cache/commons-logging/commons-logging/jars/commons-logging-1.2.jar:/home/sam/.ivy2/cache/com.google.guava/guava/jars/guava-19.0.jar:/home/sam/.ivy2/cache/commons-collections/commons-collections/jars/commons-collections-3.2.2.jar:/home/sam/.ivy2/cache/commons-lang/commons-lang/jars/commons-lang-2.6.jar:/home/sam/.ivy2/cache/org.apache.hadoop/hadoop-yarn-common/jars/hadoop-yarn-common-2.7.2.jar:/home/sam/.ivy2/cache/commons-configuration/commons-configuration/jars/commons-configuration-1.6.jar:/home/sam/.ivy2/cache/org.apache.hadoop/hadoop-auth/jars/hadoop-auth-2.7.2.jar:/home/sam/.ivy2/cache/org.slf4j/slf4j-api/jars/slf4j-api-1.7.25.jar:/home/sam/.ivy2/cache/commons-io/commons-io/jars/commons-io-2.5.jar:/home/sam/.ivy2/cache/com.google.protobuf/protobuf-java/jars/protobuf-java-2.5.0.jar:/home/sam/.ivy2/cache/org.apache.htrace/htrace-core/jars/htrace-core-3.1.0-incubating.jar:/home/sam/development/workspace/eel/eel-core/target/scala-2.11/classes io.eels.yarn.EelApplicationMaster 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr")

  val pri = Priority.newInstance(2)
  appContext.setPriority(pri)

  val resources = Resource.newInstance(1000, 1)
  appContext.setResource(resources)

  // Set up the container launch context for the application master
  val amContainer = ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null)
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