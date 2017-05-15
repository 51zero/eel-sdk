package io.eels.component

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._

case class ComponentSpec(namespace: String, impl: String)

object ComponentSpec {
  lazy val all: Seq[ComponentSpec] = ConfigFactory.load().getConfigList("eel.components").asScala.map { config =>
    ComponentSpec(config.getString("namespace"), config.getString("impl"))
  }.toVector
}