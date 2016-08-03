package io.eels.plan

import io.eels.Frame
import io.eels.Row

object ForallPlan {
  fun execute(frame: Frame, p: (Row) -> Boolean): Boolean = false
}