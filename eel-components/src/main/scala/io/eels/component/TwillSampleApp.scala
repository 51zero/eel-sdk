package io.eels.component

import java.io.{File, PrintWriter}

import com.google.common.base.Splitter
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.twill.api.AbstractTwillRunnable
import org.apache.twill.api.logging.PrinterLogHandler
import org.apache.twill.yarn.YarnTwillRunnerService

object TwillSampleApp extends App {

  val conf = new YarnConfiguration()
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/yarn-site.xml"))
  conf.reloadConfiguration()
  println(conf)

  implicit val fs = FileSystem.get(conf)

  val runnerService = new YarnTwillRunnerService(conf, "localhost:2181")
  runnerService.start()

  val yarnClasspath = Seq(conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH), YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH).mkString(",")
  val applicationClassPaths = Splitter.on(",").split(yarnClasspath)

  val controller = runnerService.prepare(new EchoServer())
    .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
    .withApplicationClassPaths(applicationClassPaths)
    .start()

  try {
    controller.awaitTerminated()
  } catch {
    case e: Throwable => e.printStackTrace()
  }
}

class EchoServer extends AbstractTwillRunnable {
  override def run(): Unit = {
    while (true) {
      new File("/home/sam/development/workspace/eel/hello").createNewFile()
    }
  }
}
