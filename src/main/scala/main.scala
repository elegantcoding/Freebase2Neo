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
  var logger:Logger = Logger("com.elegantcoding.freebase2neo")
  var idMap:IdMap = new IdMap()
  val freebaseLabel = DynamicLabel.label("freebase")
  var stage:Int = 0
  var totalIds:Int = 0
  var totalLines:Int = 0

  var freebaseFile = Settings.gzippedNTripleFile
  // TODO make these come from setting
  val inserter = BatchInserters.inserter(
    "target/batchinserter-example",
    Map[String,String](
      "neostore.nodestore.db.mapped_memory" -> "1G",
      "neostore.relationshipstore.db.mapped_memory" -> "1G",
      "neostore.propertystore.db.mapped_memory" -> "1G",
      "neostore.propertystore.db.strings" -> "1G"
    ).asJava
  )

  countIdsPass(Settings.gzippedNTripleFile)
  getIdsPass(Settings.gzippedNTripleFile)
  persistIdMap
  createNodes
  createRelationshipsPass(Settings.gzippedNTripleFile)

  inserter.shutdown
  logger.info("done!")

  def countIdsPass(filename:String) = {
    logger.info("starting stage (counting machine ids)...")
    stage += 1
    val nti = new NTripleIterable(new GZIPInputStream(new FileInputStream(filename), 65536*16))
    val start = System.currentTimeMillis
    var totalEstimate = 2624000000l // TODO make this better
    nti.foreach { triple =>
      if (triple.predicateString == "<http://rdf.freebase.com/ns/type.type.instance>") {
        totalIds += 1
      }
      totalLines = totalLines + 1
      Utils.displayProgress(stage, "count machine ids / lines", start, totalEstimate, "lines", totalLines, totalIds, "machine ids")
    }
    logger.info("done stage (counting machine ids)...")
    Utils.displayDone(stage, "count machine ids / lines", start, totalLines, "lines", totalIds, "machine ids")
  }

  def getIdsPass(filename:String) = {
    logger.info("starting stage (collecting machine ids)...")
    stage += 1
    val nti = new NTripleIterable(new GZIPInputStream(new FileInputStream(filename), 65536*16))
    var count = 0l
    val start = System.currentTimeMillis
    nti.foreach { triple =>
      if (triple.predicateString == "<http://rdf.freebase.com/ns/type.type.instance>") {
        val mid = Utils.extractId(triple.objectString)
        idMap.put(mid)
      }
      count = count + 1
      Utils.displayProgress(stage, "get machine ids", start, totalLines, "triples", count, idMap.length, "machine ids")
    }
    logger.info("done stage (collecting machine ids)...")
    Utils.displayDone(stage, "get machine ids", start, count, "triples", idMap.length, "machine ids")
  }

  def persistIdMap = {
    logger.info("starting persisting the id map...")
    idMap.done // sorts id map, etc.
    // TODO persistIdMap
    logger.info("done persisting the id map...")
  }

  def createNodes = {
    logger.info("starting creating the nodes...")
    stage += 1
    val start = System.currentTimeMillis()
    (0 until idMap.length).foreach { i =>
      inserter.createNode(i, null, freebaseLabel)
      Utils.displayProgress(stage, "create nodes", start, idMap.length, "nodes", i, i, "nodes")
    }
    Utils.displayDone(stage, "create nodes", start, idMap.length, "nodes", idMap.length, "nodes")
    logger.info("done creating the nodes...")
  }

  def createRelationshipsPass(filename:String) = {
    logger.info("starting create relationships pass...")
    stage += 1
    val nti = new NTripleIterable(new GZIPInputStream(new FileInputStream(filename), 65536*16))
    var count = 0l
    var rels = 0l
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
      Utils.displayProgress(stage, "create relationships", start, totalLines, "triples", count, rels, "relationships")
    }
    logger.info("done third pass...")
    Utils.displayDone(stage, "create relationships", start, count, "triples", rels, "relationships")
  }

  def sanitize(s:String) = {
    s.replaceAllLiterally(Settings.fbRdfPrefix, "")
    s.substring(0,s.length-1)
  }
}

