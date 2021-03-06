/**
 * _____              _                    ____  _   _
 * |  ___| __ ___  ___| |__   __ _ ___  ___|___ \| \ | | ___  ___
 * | |_ | '__/ _ \/ _ \ '_ \ / _` / __|/ _ \ __) |  \| |/ _ \/ _ \
 * |  _|| | |  __/  __/ |_) | (_| \__ \  __// __/| |\  |  __/ (_) |
 * |_|  |_|  \___|\___|_.__/ \__,_|___/\___|_____|_| \_|\___|\___/
 *
 *
 * Copyright (c) 2013-2014
 *
 * Wes Freeman [http://wes.skeweredrook.com]
 * Geoff Moes [http://elegantcoding.com]
 *
 * FreeBase2Neo is designed to import Freebase RDF triple data into Neo4J
 *
 * FreeBase2Neo is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.elegantcoding.freebase2neo

//import java.util.concurrent.ConcurrentHashMap

import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import com.elegantcoding.rdfprocessor.rdftriple.types.{RdfTripleFilter, RdfTupleFilter, RdfTriple}
import com.elegantcoding.statuscosole.{MovingAverage, ItemCountStatus, StatusInfo, StatusConsole}

import collection.JavaConverters._

import org.neo4j.unsafe.batchinsert.BatchInserter
import org.neo4j.graphdb.DynamicRelationshipType
import org.neo4j.graphdb.DynamicLabel
import com.elegantcoding.rdfprocessor.NTripleIterable
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.slf4j.Logger

import scala.collection.mutable

class Freebase2Neo(inserter: BatchInserter, settings: Settings) {
  var log = Logger(LoggerFactory.getLogger("freebase2neo"))
  var idMap: MidToIdMap = MidToIdMapBuilder().getMidToIdMap;

  //TODO: Tie these two together in Synchronized class
  val idMap2 : mutable.Map[Long, Int] = new mutable.HashMap[Long, Int]()
  var currentId = 1

  // create empty one for now
  var freebaseLabel = DynamicLabel.label("Freebase")
  var stage: Int = 0
  //var totalLines:Long = 0
  val MID_PREFIX = "<http://rdf.freebase.com/ns/m."

  var freebaseFile = settings.gzippedNTripleFile

  val SCREEN_MESSAGE = "Freebase2Neo : press ctrl-C to quit"

  //TODO: make this an Actor?
  val statusConsole = new StatusConsole(SCREEN_MESSAGE);

  def createStatusInfo(stage: Int, stageDescription: String) =
    new StatusInfo(stage,
      stageDescription,
      Seq[ItemCountStatus](
        new ItemCountStatus("lines", Seq[MovingAverage](
          new MovingAverage("(10 second moving average)", (10 * 1000)),
          new MovingAverage("(10 min moving average)", (10 * 60 * 1000)))
        )
      )
    )

  def extractId(str: String): Long = {
    base32Converter.toDecimal(str.substring(MID_PREFIX.length, str.length() - 1))
  }

  var batchInserter = inserter

  def getRdfIterable(filename: String, rdfTripleFilterOption: Option[RdfTupleFilter[RdfTriple]] = None) = rdfTripleFilterOption match {
    case None => NTripleIterable(new GZIPInputStream(new FileInputStream(filename), settings.gzipInputBufferSize))
    case Some(rdfTripleFilter) => NTripleIterable(new GZIPInputStream(new FileInputStream(filename), settings.gzipInputBufferSize), rdfTripleFilter)
  }

  /////////////////////////////////////////////////////////////////////////////////////////////
  // Filters
  // TODO: Create filters generically from settings data
  /////////////////////////////////////////////////////////////////////////////////////////////

  // TODO: this is not really a filter, also is it inherent to the freebase structure? If so it's not settings data.
  // TODO: Is this check even needed for a single pass approach?

  val getIdsPassFilter: RdfTripleFilter = (triple: RdfTriple) => {
    settings.nodeTypePredicates.contains(triple.predicateString)
  }

  val createRelationshipsFilter : RdfTripleFilter = (triple: RdfTriple) => {

    !settings.filters.predicateFilter.blacklist.equalsSeq.contains(triple.predicateString)
  }

  val createPropertiesFilter : RdfTripleFilter = (triple: RdfTriple) => {
    !settings.filters.predicateFilter.blacklist.equalsSeq.contains(triple.predicateString) &&
      (settings.filters.predicateFilter.whitelist.equalsSeq.contains(triple.predicateString) ||
        !startsWithAny(triple.predicateString, settings.filters.predicateFilter.blacklist.startsWithSeq)) &&
      (endsWithAny(triple.objectString, settings.filters.objectFilter.whitelist.endsWithSeq) ||
        startsWithAny(triple.objectString, settings.filters.objectFilter.whitelist.startsWithSeq) ||
        (!startsWithAny(triple.objectString, settings.filters.objectFilter.blacklist.startsWithSeq) &&
          !containsAny(triple.objectString, settings.filters.objectFilter.blacklist.containsSeq)))
  }

  /////////////////////////////////////////////////////////////////////////////////////////////
  // Db Creators
  /////////////////////////////////////////////////////////////////////////////////////////////

  def createNode(id: Int) =
    batchInserter.createNode(id, Map[String, java.lang.Object]("mid" -> base32Converter.toBase32(idMap.midArray(id))).asJava, freebaseLabel)


  def createNode(mid: Long) : Int = {

    val subjectNodeId = currentId + 1
    idMap2.put(mid, subjectNodeId)
    createNode(subjectNodeId)

    subjectNodeId
  }

    def createRelationship(triple: RdfTriple) = {
    val mid = extractId(triple.subjectString)
    val nodeId: Long = idMap.get(mid)
    if (nodeId >= 0) {
      // if object is an mid (this is a relationship) and
      // if predicate isn't ignored
      val objMid = extractId(triple.objectString)
      val objNodeId: Long = idMap.get(objMid)
      if (objNodeId >= 0) {
        // create relationship
        batchInserter.createRelationship(nodeId, objNodeId, DynamicRelationshipType.withName(sanitize(triple.predicateString)), null)
        1
      }
    }

    0
  }


  def createProperty(triple: RdfTriple) = {

    val mid = extractId(triple.subjectString)
    val nodeId: Long = idMap.get(mid)
    if (nodeId >= 0) {
      // create property
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
      1
    } else {
      // TODO handle labels?
    }
    0
  }

  /////////////////////////////////////////////////////////////////////////////////////////////
  // Main Creation
  /////////////////////////////////////////////////////////////////////////////////////////////

  def createDb = createDbOld

  def createDbNew = {
    log.info("source: " + settings.gzippedNTripleFile)
    log.info("db: " + settings.outputGraphPath)

    log.info("settings.nodeStoreMappedMemory : " + settings.nodeStoreMappedMemory)
    log.info("settings.relationshipStoreMappedMemory : " + settings.relationshipStoreMappedMemory)
    log.info("settings.propertyStoreMappedMemory : " + settings.propertyStoreMappedMemory)
    log.info("settings.propertyStoreStrings : " + settings.propertyStoreStrings)

    getIdsPass
    createNodes
    singlePass
    shutdown
  }

  def createDbOld = {
    log.info("source: " + settings.gzippedNTripleFile)
    log.info("db: " + settings.outputGraphPath)

    log.info("settings.nodeStoreMappedMemory : " + settings.nodeStoreMappedMemory)
    log.info("settings.relationshipStoreMappedMemory : " + settings.relationshipStoreMappedMemory)
    log.info("settings.propertyStoreMappedMemory : " + settings.propertyStoreMappedMemory)
    log.info("settings.propertyStoreStrings : " + settings.propertyStoreStrings)

    getIdsPass
    createNodes
    createRelationshipsPass
    createPropertiesPass
    shutdown
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  // Passes
  /////////////////////////////////////////////////////////////////////////////////////////////

  def singlePass() = {

    val statusInfo =
      new StatusInfo(1, "Single pass",
        Seq[ItemCountStatus](
          new ItemCountStatus("lines", Seq[MovingAverage](
            new MovingAverage("(10 second moving average)", (10 * 1000)),
            new MovingAverage("(10 min moving average)", (10 * 60 * 1000)))
          )
        )
      )

    log.info("starting create relationships pass...")
    stage += 1
    val rdfIterable = getRdfIterable(freebaseFile)
    var count = 0l
    var relationshipCount = 0l
    val start = System.currentTimeMillis
    rdfIterable.foreach((triple) => {

      if (triple.subjectString.startsWith(MID_PREFIX)) {
        // if subject is an mid

        val subjectMid = extractId(triple.subjectString)

        val subjectNodeId = idMap2.get(subjectMid).getOrElse(createNode(subjectMid))

        // if object is an mid (this is a relationship)
        if (triple.objectString.startsWith(MID_PREFIX) &&
          createRelationshipsFilter(triple)) {

          val objectMid = extractId(triple.objectString)
          val objectNodeId = idMap2.get(objectMid).getOrElse(createNode(objectMid))

          relationshipCount = relationshipCount + 1

          batchInserter.createRelationship(subjectNodeId, objectNodeId, DynamicRelationshipType.withName(sanitize(triple.predicateString)), null)

        } else {

          if (createPropertiesFilter(triple)) {

            createProperty(triple)
          }
        }
      }

      count = count + 1

      statusInfo.itemCountStatus(0).incCount
      statusConsole.displayProgress(statusInfo)
    })
    log.info("done create relationships pass...")

    statusConsole.displayDone(statusInfo)
  }

  def getIdsPass = {

    log.info("starting stage (collecting machine ids)...")

    val midToIdMapBuilder = MidToIdMapBuilder()

    stage += 1

    val statusInfo = createStatusInfo(stage, "collecting machine ids")

    val rdfIterable = getRdfIterable(freebaseFile)

    rdfIterable.foreach { triple =>
      if (getIdsPassFilter(triple)) {
        midToIdMapBuilder.put(extractId(triple.objectString))
      }

      statusInfo.itemCountStatus(0).incCount

      statusConsole.displayProgress(statusInfo)
    }

    log.info("done stage (collecting machine ids)...")

    statusConsole.displayDone(statusInfo)

    // TODO: add timing measure
    idMap = midToIdMapBuilder.getMidToIdMap
  }

  def createNodes = {

    val statusInfo = createStatusInfo(stage, "creating the nodes")

    log.info("starting creating the nodes...")
    stage += 1
    val start = System.currentTimeMillis()
    (0 until idMap.length).foreach { id =>
      createNode(id)
      statusInfo.itemCountStatus(0).incCount
      statusConsole.displayProgress(statusInfo)
    }
    statusConsole.displayDone(statusInfo)
    log.info("done creating the nodes...")
  }

  def createRelationshipsPass() = {

    val statusInfo = createStatusInfo(stage, "create relationships pass")

    log.info("starting create relationships pass...")
    stage += 1
    val rdfIterable = getRdfIterable(freebaseFile)
    var count = 0l
    var relationshipCount = 0l
    val start = System.currentTimeMillis
    rdfIterable.foreach { triple => {

      if (triple.subjectString.startsWith(MID_PREFIX)) {
        // if subject is an mid
        if (triple.objectString.startsWith(MID_PREFIX) &&
          createRelationshipsFilter(triple)) {

          relationshipCount = relationshipCount + createRelationship(triple)
        }
      }

      count = count + 1

      statusInfo.itemCountStatus(0).incCount
      statusConsole.displayProgress(statusInfo)
    }
    }

    log.info("done create relationships pass...")

    statusConsole.displayDone(statusInfo)
  }

  def createPropertiesPass() = {

    val statusInfo = createStatusInfo(stage, "create properties pass")

    log.info("starting create properties pass...")
    stage += 1
    val rdfIterable = getRdfIterable(freebaseFile)
    var count = 0l
    var propertyCount = 0l
    val start = System.currentTimeMillis
    rdfIterable.foreach { triple => {

      if (triple.subjectString.startsWith(MID_PREFIX)) {
        // if subject is an mid

        if (!triple.objectString.startsWith(MID_PREFIX)) {
          // if object is an mid (this is a relationship)

          if (createPropertiesFilter(triple)) {

            createProperty(triple)
          }
        }
      }

      count = count + 1
      statusInfo.itemCountStatus(0).incCount
      statusConsole.displayProgress(statusInfo)
    }
    }

    log.info("done create properties pass...")
    statusConsole.displayDone(statusInfo)
  }

  /////////////////////////////////////////////////////////////////////////////////////////////
  // Utils
  /////////////////////////////////////////////////////////////////////////////////////////////

  def sanitize(s: String) = {
    val s2 = s.replaceAllLiterally(settings.freebaseRdfPrefix, "")
      .replaceAllLiterally("<http://www.w3.org/1999/02/", "")
      .replaceAllLiterally("<http://www.w3.org/2000/01/", "")
      .replaceAllLiterally("<http://rdf.freebase.com/key/", "")
    s2.substring(0, s2.length - 1)
  }

  def startsWithAny(s: String, l: Seq[String]): Boolean = {
    l.foreach(v => if (s.startsWith(v)) {
      return true
    })
    return false
  }

  def endsWithAny(s: String, l: Seq[String]): Boolean = {
    l.foreach(v => if (s.endsWith(v)) {
      return true
    })
    return false
  }

  def containsAny(s: String, l: Seq[String]): Boolean = {
    l.foreach(v => if (s.contains(v)) {
      return true
    })
    return false
  }

  def shutdown = batchInserter.shutdown
}
