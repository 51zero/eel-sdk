package io.eels.component.hive

trait PartitionExpr {
  def eval(partitions: List[PartitionPart]): Boolean
}

case class PartitionEquals(name: String, value: String) extends PartitionExpr {
  override def eval(partitions: List[PartitionPart]): Boolean = partitions.contains(PartitionPart(name, value))
}

case class PartitionLt(name: String, value: String) extends PartitionExpr {
  override def eval(partitions: List[PartitionPart]): Boolean = {
    partitions.find(_.key == name).exists(_.value.compareTo(value) < 0)
  }
}

case class PartitionLte(name: String, value: String) extends PartitionExpr {
  override def eval(partitions: List[PartitionPart]): Boolean = {
    partitions.find(_.key == name).exists(_.value.compareTo(value) <= 0)
  }
}

case class PartitionGt(name: String, value: String) extends PartitionExpr {
  override def eval(partitions: List[PartitionPart]): Boolean = {
    partitions.find(_.key == name).exists(_.value.compareTo(value) > 0)
  }
}

case class PartitionGte(name: String, value: String) extends PartitionExpr {
  override def eval(partitions: List[PartitionPart]): Boolean = {
    partitions.find(_.key == name).exists(_.value.compareTo(value) >= 0)
  }
}