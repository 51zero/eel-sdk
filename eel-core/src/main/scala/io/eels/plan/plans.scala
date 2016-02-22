package io.eels.plan

import com.sksamuel.scalax.io.Using
import io.eels._

object HeadPlan extends Using {
  def apply(frame: Frame): Option[InternalRow] = {
    using(frame.buffer) { buffer =>
      buffer.iterator.take(1).toSeq.headOption
    }
  }
}

object FindPlan extends Using {
  def apply(frame: Frame, p: InternalRow => Boolean): Option[InternalRow] = {
    using(frame.buffer) { buffer =>
      buffer.iterator.find(p)
    }
  }
}

object ExistsPlan extends Using {
  def apply(frame: Frame, p: InternalRow => Boolean): Boolean = {
    using(frame.buffer) { buffer =>
      buffer.iterator.exists(p)
    }
  }
}

object ForallPlan extends Using {
  def apply(frame: Frame, p: InternalRow => Boolean): Boolean = {
    using(frame.buffer) { buffer =>
      buffer.iterator.forall(p)
    }
  }
}

object FoldPlan extends Plan {
  def apply[A](frame: Frame, a: A)(fn: (A, InternalRow) => A): A = {
    frame.buffer.iterator.foldLeft(a)(fn)
  }
}