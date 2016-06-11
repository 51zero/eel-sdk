package io.eels

import io.eels.component.Part

/**
 * A Source is a provider of data to eel.
 * A source implementation must provide two methods:
 * 1: schema() which returns an eel schema for the data source
 * 2: parts() which returns 1 or more Part instances. A part instance is a subset of the data
 * in a Source, and allows for concurrent reading of that data. A part could be a file in a multi
 * file source, or it could be a partition in a partitioned data source.
 */
interface Source : Logging {
  fun toFrame(ioThreads: Int): Frame = FrameSource(ioThreads, this)
  fun schema(): Schema
  fun parts(): List<Part>
}