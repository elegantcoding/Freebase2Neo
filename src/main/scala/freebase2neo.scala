package com.elegantcoding.freebase2neo

import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import com.elegantcoding.rdfprocessor.rdftriple.types.{RdfTupleFilter, RdfTriple}

import collection.JavaConverters._

import org.neo4j.unsafe.batchinsert.BatchInserter
import org.neo4j.graphdb.DynamicRelationshipType
import org.neo4j.graphdb.DynamicLabel
import com.elegantcoding.rdfprocessor.NTripleIterable
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.slf4j.Logger


class Freebase2Neo(inserter : BatchInserter, settings:Settings) {
  var logger = Logger(LoggerFactory.getLogger("freebase2neo"))
  var idMap:IdMap = new IdMap()
  var freebaseLabel = DynamicLabel.label("Freebase")
  var stage:Int = 0
  var totalIds:Int = 0
  var totalLines:Long = 0
  val MID_PREFIX = "<http://rdf.freebase.com/ns/m."

  var freebaseFile = settings.gzippedNTripleFile

  var batchInserter = inserter

  //TODO add 65536*16 to Settings
//  def getRdfIterable(filename : String) = NTripleIterable(new GZIPInputStream(new FileInputStream(filename), 65536 * 16))
//
//  def getRdfIterable(filename : String, rdfTripleFilter : RdfTupleFilter[RdfTriple]) = NTripleIterable(new GZIPInputStream(new FileInputStream(filename), 65536 * 16),
//    8192 * 256, (s: String) => true, rdfTripleFilter);

    def getRdfIterable(filename : String, rdfTripleFilterOption : Option[RdfTupleFilter[RdfTriple]] = None) =  rdfTripleFilterOption match {
      case None => NTripleIterable(new GZIPInputStream(new FileInputStream(filename), 65536 * 16))
      case Some(rdfTripleFilter) => NTripleIterable(new GZIPInputStream(new FileInputStream(filename), 65536 * 16), rdfTripleFilter)
    }


  def createDb = {
    countIdsPass
    getIdsPass
    persistIdMap
    createNodes
    createRelationshipsPass
    createPropertiesPass
    shutdown
  }


  def countIdsPass = {
    logger.info("starting stage (counting machine ids)...")
    stage += 1
    val rdfIterable = getRdfIterable(freebaseFile)
    val start = System.currentTimeMillis
    var totalEstimate = 2624000000l // TODO make this better based on file size?
    rdfIterable.foreach { triple =>
      if (settings.nodeTypePredicates.contains(triple.predicateString)) {
        totalIds += 1
      }
      totalLines = totalLines + 1
      Utils.displayProgress(stage, "count machine ids / lines", start, totalEstimate, "lines", totalLines, totalIds, "machine ids")
    }
    logger.info("done stage (counting machine ids)...")
    Utils.displayDone(stage, "count machine ids / lines", start, totalLines, "lines", totalIds, "machine ids")
  }

  def getIdsPass = {
    logger.info("starting stage (collecting machine ids)...")
    stage += 1
    val rdfIterable = getRdfIterable(freebaseFile)
    var count = 0l
    val start = System.currentTimeMillis
    rdfIterable.foreach { triple =>
      if (settings.nodeTypePredicates.contains(triple.predicateString)) {
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
      batchInserter.createNode(i, Map[String,java.lang.Object]("mid" -> mid2long.decode(idMap.arr(i))).asJava, freebaseLabel)
      Utils.displayProgress(stage, "create nodes", start, idMap.length, "nodes", i, i, "nodes")
    }
    Utils.displayDone(stage, "create nodes", start, idMap.length, "nodes", idMap.length, "nodes")
    logger.info("done creating the nodes...")
  }

  def createRelationshipsPass = {
    logger.info("starting create relationships pass...")
    stage += 1
    val rdfIterable = getRdfIterable(freebaseFile)
    var count = 0l
    var relationshipCount = 0l
    val start = System.currentTimeMillis
    rdfIterable.foreach {
      triple =>
      // if subject is an mid
        if (triple.subjectString.startsWith("<http://rdf.freebase.com/ns/m.")) {
          val mid = Utils.extractId(triple.subjectString)
          val nodeId: Long = idMap.get(mid)
          if (nodeId >= 0) {
            // if object is an mid (this is a relationship) and
            // if predicate isn't ignored
            if (triple.objectString.startsWith("<http://rdf.freebase.com/ns/m.") &&
              !settings.filters.predicateFilter.blacklist.equalsSeq.contains(triple.predicateString)) {
              val objMid = Utils.extractId(triple.objectString)
              val objNodeId: Long = idMap.get(objMid)
              if (objNodeId >= 0) {
                // create relationship
                batchInserter.createRelationship(nodeId, objNodeId, DynamicRelationshipType.withName(sanitize(triple.predicateString)), null)
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

  def createPropertiesPass = {
    logger.info("starting create properties pass...")
    stage += 1
    val rdfIterable = getRdfIterable(freebaseFile)
    var count = 0l
    var propertyCount = 0l
    val start = System.currentTimeMillis
    rdfIterable.foreach { triple =>
    // if subject is an mid
      if (triple.subjectString.startsWith("<http://rdf.freebase.com/ns/m.")) {
        val mid = Utils.extractId(triple.subjectString)
        val nodeId: Long = idMap.get(mid)
        if (nodeId >= 0) {
              // create property
              if(!triple.objectString.startsWith("<http://rdf.freebase.com/ns/m.") &&  // if object is an mid (this is a relationship)
                 !settings.filters.predicateFilter.blacklist.equalsSeq.contains(triple.predicateString) &&
                 (settings.filters.predicateFilter.whitelist.equalsSeq.contains(triple.predicateString) ||
                  !startsWithAny(triple.predicateString, settings.filters.predicateFilter.blacklist.startsWithSeq)) &&
                 (endsWithAny(triple.objectString, settings.filters.objectFilter.whitelist.endsWithSeq) ||
                  startsWithAny(triple.objectString, settings.filters.objectFilter.whitelist.startsWithSeq) ||
                 (!startsWithAny(triple.objectString, settings.filters.objectFilter.blacklist.startsWithSeq) &&
                  !containsAny(triple.objectString, settings.filters.objectFilter.blacklist.containsSeq)))) {
                val key = sanitize(triple.predicateString)
                // if property exists, convert it to an array of properties
                // if it's already an array, append to the array
                if (batchInserter.nodeHasProperty(nodeId, key)) {
                  var prop = batchInserter.getNodeProperties(nodeId).get(key)
                  batchInserter.removeNodeProperty(nodeId, key)
                  prop match {
                    case prop: Array[String] => {
                      batchInserter.setNodeProperty(nodeId, key, prop :+ triple.objectString)
                    }
                    case _ => {
                      batchInserter.setNodeProperty(nodeId, key, Array[String](prop.toString) :+ triple.objectString)
                    }
                  }
                } else {
                  batchInserter.setNodeProperty(nodeId, key, triple.objectString)
                }
                propertyCount += 1
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
    val s2 = s.replaceAllLiterally(settings.freebaseRdfPrefix, "")
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

  def shutdown =  batchInserter.shutdown

}