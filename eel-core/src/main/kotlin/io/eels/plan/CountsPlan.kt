package io.eels.plan

import io.eels.Frame
import org.apache.hadoop.hdfs.server.namenode.Content

object CountsPlan {
  fun execute(frame: Frame): Map<String, Content.Counts> = mapOf()
}