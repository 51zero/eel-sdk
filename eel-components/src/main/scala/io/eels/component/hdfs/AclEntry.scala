package io.eels.component.hdfs

case class AclSpec(entries: List[AclEntry])

case class AclEntry(name: String, `type`: String, action: String)
