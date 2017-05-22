package io.eels.component

import java.util.concurrent.ConcurrentHashMap

class EelRegistry {

  private val map = new ConcurrentHashMap[String, Any]

  def put(name: String, value: Any): Unit = map.put(name, value)
  def get[T](name: String): T = map.get(name).asInstanceOf[T]
}
