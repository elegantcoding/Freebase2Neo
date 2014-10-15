package com.elegantcoding.freebase2neo

import collection.JavaConverters._

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory
import org.neo4j.unsafe.batchinsert.BatchInserters

object Main extends App {
  var logger = Logger(LoggerFactory.getLogger("freebase2neo.main"))
  val inserter = BatchInserters.inserter(
    Settings.outputGraphPath,
    Map[String,String](
      "neostore.nodestore.db.mapped_memory" -> "1G",
      "neostore.relationshipstore.db.mapped_memory" -> "16G",
      "neostore.propertystore.db.mapped_memory" -> "16G",
      "neostore.propertystore.db.strings" -> "16G"
    ).asJava
  )
  val freebase2neo = new Freebase2Neo(inserter)
  freebase2neo.countIdsPass(Settings.gzippedNTripleFile)
  freebase2neo.getIdsPass(Settings.gzippedNTripleFile)
  freebase2neo.persistIdMap
  freebase2neo.createNodes
  freebase2neo.createRelationshipsPass(Settings.gzippedNTripleFile)
  freebase2neo.createPropertiesPass(Settings.gzippedNTripleFile)
  freebase2neo.shutdown
  logger.info("done!")
}

