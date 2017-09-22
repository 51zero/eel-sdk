package io.eels.yarn

import java.nio.ByteBuffer
import java.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.service.{Service, ServiceStateChangeListener}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{Container, ContainerId, ContainerLaunchContext, ContainerStatus, FinalApplicationStatus, LocalResource, NodeReport, Priority, Resource}
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.conf.YarnConfiguration

import scala.collection.concurrent.TrieMap

object EelApplicationMaster extends App {
  println("Starting eel app master")

  val conf = new YarnConfiguration()
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/yarn-site.xml"))
  conf.reloadConfiguration()
  println(conf)

  implicit val fs = FileSystem.get(conf)

  val nmClient = new NMClientAsyncImpl(new NMClientAsync.CallbackHandler {
    override def onContainerStarted(containerId: ContainerId, allServiceResponse: java.util.Map[String, ByteBuffer]): Unit = println(allServiceResponse)
    override def onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus): Unit = println(containerStatus)
    override def onContainerStopped(containerId: ContainerId): Unit = println(containerId)
    override def onStartContainerError(containerId: ContainerId, t: Throwable): Unit = println(t)
    override def onStopContainerError(containerId: ContainerId, t: Throwable): Unit = println(t)
    override def onGetContainerStatusError(containerId: ContainerId, t: Throwable): Unit = println(t)
  })
  nmClient.init(conf)
  println("Starting nmClientAsync")
  nmClient.start()

  import scala.collection.JavaConverters._

  val pending = TrieMap.empty[String, Boolean]

  val amRMClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](1000, new CallbackHandler {
    override def onError(e: Throwable): Unit = println(e)
    override def getProgress: Float = 25
    override def onShutdownRequest(): Unit = println("Shutdown request")
    override def onNodesUpdated(updatedNodes: java.util.List[NodeReport]): Unit = println("Updated nodes=" + updatedNodes)
    override def onContainersCompleted(statuses: java.util.List[ContainerStatus]): Unit = {
      println("Containers completed=" + statuses)
      statuses.asScala.foreach { container =>
        pending.update(container.getContainerId.toString, true)
      }
      if (!pending.values.exists(_ == false)) {
        complete()
      }
    }
    override def onContainersAllocated(containers: java.util.List[Container]): Unit = {
      containers.asScala.foreach { container =>

        val localResources = new java.util.HashMap[String, LocalResource]()
        val env = new java.util.HashMap[String, String]()
        val commands = util.Arrays.asList("java -cp /home/sam/.ivy2/cache/commons-codec/commons-codec/jars/commons-codec-1.10.jar:/home/sam/.ivy2/cache/commons-cli/commons-cli/jars/commons-cli-1.3.1.jar:/home/sam/.ivy2/cache/javax.servlet/javax.servlet-api/jars/javax.servlet-api-3.1.0.jar:/home/sam/.ivy2/cache/org.apache.hadoop/hadoop-hdfs/jars/hadoop-hdfs-2.7.2.jar:/home/sam/.gradle/caches/modules-2/files-2.1/org.scala-lang/scala-library/2.11.8/ddd5a8bced249bedd86fb4578a39b9fb71480573/scala-library-2.11.8.jar:/home/sam/.ivy2/cache/org.apache.hadoop/hadoop-yarn-client/jars/hadoop-yarn-client-2.7.2.jar:/home/sam/.ivy2/cache/org.apache.hadoop/hadoop-yarn-api/jars/hadoop-yarn-api-2.7.2.jar:/home/sam/.ivy2/cache/org.apache.hadoop/hadoop-common/jars/hadoop-common-2.7.2.jar:/home/sam/.ivy2/cache/commons-logging/commons-logging/jars/commons-logging-1.2.jar:/home/sam/.ivy2/cache/com.google.guava/guava/jars/guava-19.0.jar:/home/sam/.ivy2/cache/commons-collections/commons-collections/jars/commons-collections-3.2.2.jar:/home/sam/.ivy2/cache/commons-lang/commons-lang/jars/commons-lang-2.6.jar:/home/sam/.ivy2/cache/org.apache.hadoop/hadoop-yarn-common/jars/hadoop-yarn-common-2.7.2.jar:/home/sam/.ivy2/cache/commons-configuration/commons-configuration/jars/commons-configuration-1.6.jar:/home/sam/.ivy2/cache/org.apache.hadoop/hadoop-auth/jars/hadoop-auth-2.7.2.jar:/home/sam/.ivy2/cache/org.slf4j/slf4j-api/jars/slf4j-api-1.7.25.jar:/home/sam/.ivy2/cache/commons-io/commons-io/jars/commons-io-2.5.jar:/home/sam/.ivy2/cache/com.google.protobuf/protobuf-java/jars/protobuf-java-2.5.0.jar:/home/sam/.ivy2/cache/org.apache.htrace/htrace-core/jars/htrace-core-3.1.0-incubating.jar:/home/sam/development/workspace/eel/eel-core/target/scala-2.11/classes io.eels.yarn.EelContainerTask 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr")

        val partitionContainer = ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null)
        nmClient.startContainerAsync(container, partitionContainer)

        pending.update(container.getId.toString, false)
      }
    }
  })
  amRMClient.init(conf)
  println("Starting am rm client")
  amRMClient.start()

  def complete(): Unit = {
    amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Completed", "")
    amRMClient.stop()
    nmClient.stop()
  }

  amRMClient.registerServiceListener(new ServiceStateChangeListener {
    override def stateChanged(service: Service): Unit = println(service)
  })

  println("Cluster node count=" + amRMClient.getClusterNodeCount)

  // Set up resource type requirements
  // For now, memory and CPU are supported so we set memory and cpu requirements

  val credentials = new Credentials()

  val tokens = fs.addDelegationTokens("sam", credentials)
  if (tokens != null) {
    for (token <- tokens) {
      println("Got dt for " + fs.getUri() + "; " + token)
    }
  }

  amRMClient.registerApplicationMaster("", 8080, "apptrackingurl")

  val capability = Resource.newInstance(250, 1)
  val containerRequest = new AMRMClient.ContainerRequest(capability, null, null, Priority.newInstance(2))
  println("Requested container ask: " + containerRequest.toString)
  amRMClient.addContainerRequest(containerRequest)
}
