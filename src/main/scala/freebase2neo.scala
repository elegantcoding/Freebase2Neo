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
  var idMap : MidToIdMap = MidToIdMapBuilder().getMidToIdMap; // create empty one for now
  var freebaseLabel = DynamicLabel.label("Freebase")
  var stage:Int = 0
  //var totalLines:Long = 0
  val MID_PREFIX = "<http://rdf.freebase.com/ns/m."

  var freebaseFile = settings.gzippedNTripleFile

  //TODO: make this an Actor
  val statusConsole = new StatusConsole();


  def createStatusInfo(stage : Int, stageDescription : String) =
    new StatusInfo(stage,
                   stageDescription,
                   Seq[ItemCountStatus](
                      new ItemCountStatus("line", Seq[MovingAverage](
                        new MovingAverage("(10 second moving average)",(10 * 1000)),
                        new MovingAverage("(10 min moving average)", (10 * 60 * 1000)))
                      )
                   )
    )




      def extractId(str:String):Long = {
    mid2long.encode(str.substring(MID_PREFIX.length, str.length()-1))
  }


  var batchInserter = inserter

  //TODO add 65536*16 to Settings

    def getRdfIterable(filename : String, rdfTripleFilterOption : Option[RdfTupleFilter[RdfTriple]] = None) =  rdfTripleFilterOption match {
      case None => NTripleIterable(new GZIPInputStream(new FileInputStream(filename), 65536 * 16))
      case Some(rdfTripleFilter) => NTripleIterable(new GZIPInputStream(new FileInputStream(filename), 65536 * 16), rdfTripleFilter)
    }


  def createDb = {
    getIdsPass
    persistIdMap
    createNodes
    createRelationshipsPass
    createPropertiesPass
    shutdown
  }

  val midToIdMapBuilder = MidToIdMapBuilder()


  def getIdsPass = {
    logger.info("starting stage (collecting machine ids)...")

    stage += 1

    val statusInfo = createStatusInfo(stage, "collecting machine ids")

//    val midToIdMapBuilder = MidToIdMapBuilder()

    val rdfIterable = getRdfIterable(freebaseFile)
    //var count = 0l
    //val start = System.currentTimeMillis
    rdfIterable.foreach { triple =>
      if (settings.nodeTypePredicates.contains(triple.predicateString)) {
        midToIdMapBuilder.put(extractId(triple.objectString))
      }

      statusInfo.itemCountStatus(0).incCount

      statusConsole.displayProgress(statusInfo)
    }
    logger.info("done stage (collecting machine ids)...")

    statusConsole.displayDone(statusInfo)

    // TODO: add timing measure
    //idMap = midToIdMapBuilder.getMidToIdMap
  }


  // TODO: Unneeded step, leaving for timing check, add to previous step with timing measure
  def persistIdMap = {
    logger.info("starting persisting the id map...")
    idMap = midToIdMapBuilder.getMidToIdMap
    logger.info("done persisting the id map...")
  }

  def createNodes = {

    val statusInfo = createStatusInfo(stage, "collecting machine ids")

    logger.info("starting creating the nodes...")
    stage += 1
    val start = System.currentTimeMillis()
    (0 until idMap.length).foreach { i =>
      batchInserter.createNode(i, Map[String,java.lang.Object]("mid" -> mid2long.decode(idMap.midArray(i))).asJava, freebaseLabel)
      statusInfo.itemCountStatus(0).incCount
      statusConsole.displayProgress(statusInfo)
    }
    statusConsole.displayDone(statusInfo)
    logger.info("done creating the nodes...")
  }

  def createRelationshipsPass = {

    val statusInfo = createStatusInfo(stage, "collecting machine ids")

    logger.info("starting create relationships pass...")
    stage += 1
    val rdfIterable = getRdfIterable(freebaseFile)
    var count = 0l
    var relationshipCount = 0l
    val start = System.currentTimeMillis
    rdfIterable.foreach {
      triple =>
      // if subject is an mid
        if (triple.subjectString.startsWith(MID_PREFIX)) {
          val mid = extractId(triple.subjectString)
          val nodeId: Long = idMap.get(mid)
          if (nodeId >= 0) {
            // if object is an mid (this is a relationship) and
            // if predicate isn't ignored
            if (triple.objectString.startsWith(MID_PREFIX) &&
              !settings.filters.predicateFilter.blacklist.equalsSeq.contains(triple.predicateString)) {
              val objMid = extractId(triple.objectString)
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

        statusInfo.itemCountStatus(0).incCount
        statusConsole.displayProgress(statusInfo)

      //Utils.displayProgress(stage, "create relationships", start, totalLines, "triples", count, relationshipCount, "relationships")
    }
    logger.info("done create relationships pass...")

    statusConsole.displayDone(statusInfo)
  }

  def createPropertiesPass = {

    val statusInfo = createStatusInfo(stage, "collecting machine ids")

    logger.info("starting create properties pass...")
    stage += 1
    val rdfIterable = getRdfIterable(freebaseFile)
    var count = 0l
    var propertyCount = 0l
    val start = System.currentTimeMillis
    rdfIterable.foreach { triple =>
    // if subject is an mid
      if (triple.subjectString.startsWith(MID_PREFIX)) {
        val mid = extractId(triple.subjectString)
        val nodeId: Long = idMap.get(mid)
        if (nodeId >= 0) {
              // create property
              if(!triple.objectString.startsWith(MID_PREFIX) &&  // if object is an mid (this is a relationship)
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
      statusInfo.itemCountStatus(0).incCount
      statusConsole.displayProgress(statusInfo)
    }
    logger.info("done create properties pass...")
    statusConsole.displayDone(statusInfo)
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