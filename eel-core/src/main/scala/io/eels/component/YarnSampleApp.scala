package io.eels.component

import java.nio.ByteBuffer
import java.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.service.{Service, ServiceStateChangeListener}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
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
  val commands = util.Arrays.asList(" echo 'hello' 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr")

  val pri = Priority.newInstance(0)
  appContext.setPriority(pri)

  val resources = Resource.newInstance(100, 2)
  appContext.setResource(resources)

  // Set up the container launch context for the application master
  val amContainer = ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null)
  appContext.setAMContainerSpec(amContainer)

  yarnClient.submitApplication(appContext)

  val amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, new CallbackHandler {
    override def onError(e: Throwable): Unit = println(e)
    override def getProgress: Float = 25
    override def onShutdownRequest(): Unit = ()
    override def onNodesUpdated(updatedNodes: util.List[NodeReport]): Unit = println(updatedNodes)
    override def onContainersCompleted(statuses: util.List[ContainerStatus]): Unit = println(statuses)
    override def onContainersAllocated(containers: util.List[Container]): Unit = println(containers)
  })
  amRMClient.init(conf)
  amRMClient.start()

  val nmClientAsync = new NMClientAsyncImpl(new NMClientAsync.CallbackHandler {
    override def onContainerStarted(containerId: ContainerId, allServiceResponse: util.Map[String, ByteBuffer]): Unit = println(allServiceResponse)
    override def onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus): Unit = println(containerStatus)
    override def onContainerStopped(containerId: ContainerId): Unit = println(containerId)
    override def onStartContainerError(containerId: ContainerId, t: Throwable): Unit = println(t)
    override def onStopContainerError(containerId: ContainerId, t: Throwable): Unit = println(t)
    override def onGetContainerStatusError(containerId: ContainerId, t: Throwable): Unit = println(t)
  })
  nmClientAsync.init(conf)
  nmClientAsync.start()

  amRMClient.registerServiceListener(new ServiceStateChangeListener {
    override def stateChanged(service: Service): Unit = println(service)
  })

  val credentials = new Credentials()

  // For now, only getting tokens for the default file-system.
  val tokens = fs.addDelegationTokens("sammy", credentials)
  if (tokens != null) {
    for (token <- tokens) {
      println("Got dt for " + fs.getUri() + "; " + token)
    }
  }
  val dob = new DataOutputBuffer()
  credentials.writeTokenStorageToStream(dob)
  val fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength())
  amContainer.setTokens(fsTokens)

  val response = amRMClient.registerApplicationMaster("", 0, "")
}
