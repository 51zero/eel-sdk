package io.eels.component

import com.sksamuel.exts.OptionImplicits._
import io.eels.Source

case class ComponentUri(namespace: String, path: String, params: Map[String, String]) {
  // strip the prefix and look it up in the component map
  def source: Source = {
    val spec = ComponentSpec.all.find(_.namespace == namespace).getOrError(s"No component matching $namespace could be found")
    val clazz = Class.forName(spec.impl)
    val component = clazz.newInstance().asInstanceOf[Component]
    component.source(params)
  }
}

object ComponentUri {

  private val Regex = "(.*?):(.*?)(\\?.*?)?".r

  def apply(str: String): ComponentUri = {
    str match {
      case Regex(namespace, path, queryString) if queryString != null =>
        val params = queryString.stripPrefix("?").split('&').map { kv =>
          kv.split('=') match {
            case Array(key) => key -> ""
            case Array(key, value) => key -> value
          }
        }.toMap
        ComponentUri(namespace, path, params)
      case Regex(namespace, path, _) => ComponentUri(namespace, path, Map.empty)
      case _ => sys.error(s"Invalid uri $str")
    }
  }
}