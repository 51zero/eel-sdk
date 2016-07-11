package io.eels

import io.eels.component.Part
import io.eels.schema.Schema
import io.eels.util.Logging

/**
 * A Source is a provider of data.
 *
 * A source implementation must provide two methods:
 *
 * 1: schema() which returns an eel schema for the data source.
 *
 * 2: parts() which returns zero or more Part instances representing the data.
 *
 * A part instance is a subset of the data in a Source, and allows for concurrent
 * reading of that data. For example a part could be a single file in a multi-file source, or
 * a partition in a partitioned source.
 */
interface Source : Logging {
  fun toFrame(ioThreads: Int): Frame = FrameSource(ioThreads, this)
  fun schema(): Schema
  fun parts(): List<Part>
}