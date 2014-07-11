package com.elegantcoding.freebase2neo

import com.elegantcoding.rdfprocessor.NTripleIterable
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import grizzled.slf4j.Logger
import org.neo4j.unsafe.batchinsert.BatchInserters
import org.neo4j.graphdb.DynamicRelationshipType

import collection.JavaConverters._
import org.neo4j.graphdb.DynamicLabel

object Main extends App {
  val logger = Logger("com.elegantcoding.freebase2neo")
  val idMap = new IdMap()
  val freebaseLabel = DynamicLabel.label("freebase")
  val inserter = BatchInserters.inserter(
    "target/batchinserter-example",
    Map[String,String](
      "neostore.nodestore.db.mapped_memory" -> "1G",
      "neostore.relationshipstore.db.mapped_memory" -> "1G",
      "neostore.propertystore.db.mapped_memory" -> "1G",
      "neostore.propertystore.db.strings" -> "1G"
    ).asJava
  )

  getIdsPass
  persistIdMap
  createNodes
  createRelationshipsPass

  inserter.shutdown
  logger.info("done!")

  def getIdsPass = {
    logger.info("starting pass (collecting machine ids)...")
    val nti = new NTripleIterable(new GZIPInputStream(new FileInputStream(Settings.gzippedNTripleFile), 65536*16))
    var count = 0l
    val start = System.currentTimeMillis
    val total = 2625000000l
    nti.foreach { triple =>
      if (triple.predicateString == "<http://rdf.freebase.com/ns/type.type.instance>") {
        val mid = Utils.extractId(triple.objectString)
        idMap.put(mid)
      }
      count = count + 1
      Utils.displayProgress(1, "get machine ids", start, total, "triples", count, idMap.length, "machine ids")
    }
    logger.info("done pass (collecting machine ids)...")
    Utils.displayDone(1, "get machine ids", start, count, "triples", idMap.length, "machine ids")
  }

  def persistIdMap = {
    logger.info("starting persisting the id map...")
    idMap.done // sorts id map, etc.
    // TODO persistIdMap
    logger.info("done persisting the id map...")
  }

  def createNodes = {
    logger.info("starting creating the nodes...")
    val start = System.currentTimeMillis()
    (0 until idMap.length).foreach { i =>
      inserter.createNode(i, null, freebaseLabel)
      Utils.displayProgress(2, "create nodes", start, idMap.length, "nodes", i, i, "nodes")
    }
    Utils.displayDone(2, "create nodes", start, idMap.length, "nodes", idMap.length, "nodes")
    logger.info("done creating the nodes...")
  }

  def createRelationshipsPass = {
    logger.info("starting create relationships pass...")
    val nti = new NTripleIterable(new GZIPInputStream(new FileInputStream(Settings.gzippedNTripleFile), 65536*16))
    var count = 0l
    var rels = 0l
    val total = 2625000000l
    val start = System.currentTimeMillis
    nti.foreach { triple =>
      // if subject is an mid
      if (triple.subjectString.startsWith("<http://rdf.freebase.com/ns/m.")) {
        val mid = Utils.extractId(triple.subjectString)
        val nodeId: Long = idMap.get(mid)
        if (nodeId >= 0) {
          // if predicate isn't ignored
          if (!Settings.ignorePredicates.contains(triple.predicateString)) {
            // if object is an mid (this is a relationship)
            if (triple.objectString.startsWith("<http://rdf.freebase.com/ns/m.")) {
              val objMid = Utils.extractId(triple.objectString)
              val objNodeId: Long = idMap.get(objMid)
              if (objNodeId >= 0) {
                // create relationship
                inserter.createRelationship(nodeId, objNodeId, DynamicRelationshipType.withName(sanitize(triple.predicateString)), null)
                rels += 1
              }
            } else {
              // TODO handle properties?
            }
          }
        } else {
          // TODO handle labels?
        }
      }
      count = count + 1
      Utils.displayProgress(3, "create relationships", start, total, "triples", count, rels, "relationships")
    }
    logger.info("done third pass...")
    Utils.displayDone(3, "create relationships", start, count, "triples", rels, "relationships")
  }

  def sanitize(s:String) = {
    s.replaceAllLiterally(Settings.fbRdfPrefix, "")
    s.substring(0,s.length-1)
  }
}

