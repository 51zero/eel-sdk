package io.eels

import io.eels.component.Part

interface Source : Logging {

  fun schema(): Schema
  fun parts(): List<Part>

  fun toFrame(ioThreads: Int): Frame = FrameSource(ioThreads, this)
}