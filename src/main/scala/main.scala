package com.elegantcoding.freebase2neo

import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import collection.JavaConverters._

import org.neo4j.unsafe.batchinsert.BatchInserters
import org.neo4j.graphdb.DynamicRelationshipType
import org.neo4j.graphdb.DynamicLabel
import com.typesafe.scalalogging._
import com.elegantcoding.rdfprocessor.NTripleIterable
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.slf4j.Logger

object Main extends App with Logging {
  var logger = Logger(LoggerFactory.getLogger("freebase2neo.main"))
  var idMap:IdMap = new IdMap()
  var freebaseLabel = DynamicLabel.label("freebase")
  var stage:Int = 0
  var totalIds:Int = 0
  var totalLines:Long = 0
  var dbpath = Settings.outputGraphPath
  //var MID_PREFIX = "<http://rdf.freebase.com/ns/m."

  var freebaseFile = Settings.gzippedNTripleFile
  // TODO make these come from setting
  var inserter = BatchInserters.inserter(
    dbpath,
    Map[String,String](
      "neostore.nodestore.db.mapped_memory" -> "1G",
      "neostore.relationshipstore.db.mapped_memory" -> "16G",
      "neostore.propertystore.db.mapped_memory" -> "16G",
      "neostore.propertystore.db.strings" -> "16G"
    ).asJava
  )

  countIdsPass(Settings.gzippedNTripleFile)
  getIdsPass(Settings.gzippedNTripleFile)
  persistIdMap
  createNodes
  createRelationshipsPass(Settings.gzippedNTripleFile)
  createPropertiesPass(Settings.gzippedNTripleFile)

  inserter.shutdown
  logger.info("done!")

  def countIdsPass(filename:String) = {
    logger.info("starting stage (counting machine ids)...")
    stage += 1
    val nti = new NTripleIterable(new GZIPInputStream(new FileInputStream(filename), 65536*16))
    val start = System.currentTimeMillis
    var totalEstimate = 2624000000l // TODO make this better based on file size?
    nti.foreach { triple =>
      if (Settings.nodeTypePredicates.contains(triple.predicateString)) {
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
      if (Settings.nodeTypePredicates.contains(triple.predicateString)) {
        idMap.put(Utils.extractId(triple.objectString))
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
      inserter.createNode(i, Map[String,java.lang.Object]("mid" -> mid2long.decode(idMap.arr(i))).asJava, freebaseLabel)
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
    var relationshipCount = 0l
    val start = System.currentTimeMillis
    nti.foreach {
      triple =>
      // if subject is an mid
        if (triple.subjectString.startsWith("<http://rdf.freebase.com/ns/m.")) {
          val mid = Utils.extractId(triple.subjectString)
          val nodeId: Long = idMap.get(mid)
          if (nodeId >= 0) {
            // if object is an mid (this is a relationship) and
            // if predicate isn't ignored
            if (triple.objectString.startsWith("<http://rdf.freebase.com/ns/m.") &&
              !Settings.filters.predicate.blacklist.equalsSeq.contains(triple.predicateString)) {
              val objMid = Utils.extractId(triple.objectString)
              val objNodeId: Long = idMap.get(objMid)
              if (objNodeId >= 0) {
                // create relationship
                inserter.createRelationship(nodeId, objNodeId, DynamicRelationshipType.withName(sanitize(triple.predicateString)), null)
                relationshipCount += 1
              }
            }
          }
        }
        count = count + 1
        Utils.displayProgress(stage, "create relationships", start, totalLines, "triples", count, relationshipCount, "relationships")
    }
    logger.info("done create relationships pass...")
    Utils.displayDone(stage, "create relationships", start, count, "triples", relationshipCount, "relationships")
  }

  def createPropertiesPass(filename:String) = {
    logger.info("starting create properties pass...")
    stage += 1
    val nti = new NTripleIterable(new GZIPInputStream(new FileInputStream(filename), 65536*16))
    var count = 0l
    var propertyCount = 0l
    val start = System.currentTimeMillis
    nti.foreach { triple =>
    // if subject is an mid
      if (triple.subjectString.startsWith("<http://rdf.freebase.com/ns/m.")) {
        val mid = Utils.extractId(triple.subjectString)
        val nodeId: Long = idMap.get(mid)
        if (nodeId >= 0) {
          // if predicate isn't ignored
          if (!Settings.filters.predicate.blacklist.equalsSeq.contains(triple.predicateString)) {
            // if object is an mid (this is a relationship)
            if (triple.objectString.startsWith("<http://rdf.freebase.com/ns/m.")) {
              // do nothing
            } else {
              // create property
              val key = sanitize(triple.predicateString)
              if((Settings.filters.predicate.whitelist.equalsSeq.contains(triple.predicateString) || !startsWithAny(triple.predicateString, Settings.filters.predicate.blacklist.startsWithSeq)) &&
                 (endsWithAny(triple.objectString, Settings.filters.obj.whitelist.endsWithSeq) || startsWithAny(triple.objectString, Settings.filters.obj.whitelist.startsWithSeq) || (!startsWithAny(triple.objectString, Settings.filters.obj.blacklist.startsWithSeq) && !containsAny(triple.objectString, Settings.filters.obj.blacklist.containsSeq)))
                ) {
                // if property exists, convert it to an array of properties
                // if it's already an array, append to the array
                if (inserter.nodeHasProperty(nodeId, key)) {
                  var prop = inserter.getNodeProperties(nodeId).get(key)
                  inserter.removeNodeProperty(nodeId, key)
                  prop match {
                    case prop: Array[String] => {
                      inserter.setNodeProperty(nodeId, key, prop :+ triple.objectString)
                    }
                    case _ => {
                      inserter.setNodeProperty(nodeId, key, Array[String](prop.toString) :+ triple.objectString)
                    }
                  }
                } else {
                  inserter.setNodeProperty(nodeId, key, triple.objectString)
                }
                propertyCount += 1
              }
            }
          }
        } else {
          // TODO handle labels?
        }
      }
      count = count + 1
      Utils.displayProgress(stage, "create properties", start, totalLines, "triples", count, propertyCount, "properties")
    }
    logger.info("done create properties pass...")
    Utils.displayDone(stage, "create properties", start, count, "triples", propertyCount, "properties")
  }

  def sanitize(s:String) = {
    val s2 = s.replaceAllLiterally(Settings.freebaseRdfPrefix, "")
      .replaceAllLiterally("<http://www.w3.org/1999/02/", "")
      .replaceAllLiterally("<http://www.w3.org/2000/01/", "")
      .replaceAllLiterally("<http://rdf.freebase.com/key/", "")
    s2.substring(0,s2.length-1)
  }

  def startsWithAny(s:String, l:Seq[String]):Boolean = {
    l.foreach(v => if(s.startsWith(v)) {return true})
    return false
  }

  def endsWithAny(s:String, l:Seq[String]):Boolean = {
    l.foreach(v => if(s.endsWith(v)) {return true})
    return false
  }

  def containsAny(s:String, l:Seq[String]):Boolean = {
    l.foreach(v => if(s.contains(v)) {return true})
    return false
  }
}

