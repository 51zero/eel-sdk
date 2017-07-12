package io.eels.component.hdfs

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import com.sksamuel.exts.Logging
import io.eels.util.HdfsIterator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.client.HdfsAdmin
import org.apache.hadoop.hdfs.inotify.Event

import scala.concurrent.duration._
import scala.util.control.NonFatal

class HdfsWatcher(path: Path, callback: FileCallback)
                 (implicit fs: FileSystem, conf: Configuration) extends Logging {

  private val files = HdfsIterator.remote(fs.listFiles(path, false)).map(_.getPath).toBuffer
  files.foreach(callback.onStart)

  private val executor = Executors.newSingleThreadExecutor()
  private val running = new AtomicBoolean(true)
  private val interval = 5.seconds

  val admin = new HdfsAdmin(path.toUri, conf)
  val eventStream = admin.getInotifyEventStream

  executor.submit(new Runnable {
    override def run(): Unit = {
      while (running.get) {
        try {
          Thread.sleep(interval.toMillis)
          val events = eventStream.take
          for (event <- events.getEvents) {
            event match {
              case create: Event.CreateEvent => callback.onCreate(create)
              case append: Event.AppendEvent => callback.onAppend(append)
              case rename: Event.RenameEvent => callback.onRename(rename)
              case close: Event.CloseEvent => callback.onClose(close)
              case _ =>
            }
          }
        } catch {
          case NonFatal(e) => logger.error("Error while polling fs", e)
        }
      }
    }
  })

  def stop(): Unit = {
    running.set(false)
    executor.shutdownNow()
  }
}

trait FileCallback {
  def onStart(path: Path): Unit
  def onClose(close: Event.CloseEvent): Unit
  def onRename(rename: Event.RenameEvent): Unit
  def onAppend(append: Event.AppendEvent): Unit
  def onCreate(path: Event.CreateEvent): Unit
}