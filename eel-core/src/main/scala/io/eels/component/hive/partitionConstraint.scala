package io.eels.component.hive

trait PartitionConstraint {
  // returns true if the partition name and value matches one of the given partition parts
  def eval(partitions: Seq[PartitionPart]): Boolean
}

case class PartitionEquals(name: String, value: String) extends PartitionConstraint {
  override def eval(partitions: Seq[PartitionPart]): Boolean = {
    partitions.contains(PartitionPart(name, value))
  }
}

case class PartitionLt(name: String, value: String) extends PartitionConstraint {
  override def eval(partitions: Seq[PartitionPart]): Boolean = {
    partitions.find(_.key == name).exists(_.value.compareTo(value) < 0)
  }
}

case class PartitionLte(name: String, value: String) extends PartitionConstraint {
  override def eval(partitions: Seq[PartitionPart]): Boolean = {
    partitions.find(_.key == name).exists(_.value.compareTo(value) <= 0)
  }
}

case class PartitionGt(name: String, value: String) extends PartitionConstraint {
  override def eval(partitions: Seq[PartitionPart]): Boolean = {
    partitions.find(_.key == name).exists(_.value.compareTo(value) > 0)
  }
}

case class PartitionGte(name: String, value: String) extends PartitionConstraint {
  override def eval(partitions: Seq[PartitionPart]): Boolean = {
    partitions.find(_.key == name).exists(_.value.compareTo(value) >= 0)
  }
}