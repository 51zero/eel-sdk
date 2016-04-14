package io.eels.component

import io.eels.Schema

interface Source {
  fun schema(): Schema
  fun parts(): List<io.eels.component.Part>
}
