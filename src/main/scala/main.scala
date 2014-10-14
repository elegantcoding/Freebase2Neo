package com.elegantcoding.freebase2neo

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

object Main extends App {
  var logger = Logger(LoggerFactory.getLogger("freebase2neo.main"))
  Freebase2Neo.countIdsPass(Settings.gzippedNTripleFile)
  Freebase2Neo.getIdsPass(Settings.gzippedNTripleFile)
  Freebase2Neo.persistIdMap
  Freebase2Neo.createNodes
  Freebase2Neo.createRelationshipsPass(Settings.gzippedNTripleFile)
  Freebase2Neo.createPropertiesPass(Settings.gzippedNTripleFile)
  Freebase2Neo.shutdown
  logger.info("done!")
}

