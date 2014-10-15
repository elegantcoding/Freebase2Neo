package com.elegantcoding.freebase2neo

import collection.JavaConverters._

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory
import org.neo4j.unsafe.batchinsert.BatchInserters

object Main extends App {
  val settings = new Settings
  var logger = Logger(LoggerFactory.getLogger("freebase2neo.main"))
  val inserter = BatchInserters.inserter(
    settings.outputGraphPath,
    Map[String,String](
      "neostore.nodestore.db.mapped_memory" -> "1G",
      "neostore.relationshipstore.db.mapped_memory" -> "16G",
      "neostore.propertystore.db.mapped_memory" -> "16G",
      "neostore.propertystore.db.strings" -> "16G"
    ).asJava
  )
  val freebase2neo = new Freebase2Neo(inserter, settings)
  freebase2neo.createDb
  logger.info("done!")
}

