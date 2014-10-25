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
import java.util.concurrent.CountDownLatch


class Freebase2Neo(inserter: BatchInserter, settings: Settings) {
  var logger = Logger(LoggerFactory.getLogger("freebase2neo"))
  var idMap: IdMap = new IdMap()
  var freebaseLabel = DynamicLabel.label("Freebase")
  var stage: Int = 0
  var totalIds: Int = 0
  var totalLines: Long = 2700000000L
  val MID_PREFIX = "<http://rdf.freebase.com/ns/m."

  var freebaseFile = settings.gzippedNTripleFile

  var batchInserter = inserter

  //TODO add 65536*16 to Settings
  //  def getRdfIterable(filename : String) = NTripleIterable(new GZIPInputStream(new FileInputStream(filename), 65536 * 16))
  //
  //  def getRdfIterable(filename : String, rdfTripleFilter : RdfTupleFilter[RdfTriple]) = NTripleIterable(new GZIPInputStream(new FileInputStream(filename), 65536 * 16),
  //    8192 * 256, (s: String) => true, rdfTripleFilter);

  def getRdfIterable(filename: String, rdfTripleFilterOption: Option[RdfTupleFilter[RdfTriple]] = None) = rdfTripleFilterOption match {
    case None => NTripleIterable(new GZIPInputStream(new FileInputStream(filename), 65536 * 16))
    case Some(rdfTripleFilter) => NTripleIterable(new GZIPInputStream(new FileInputStream(filename), 65536 * 16), rdfTripleFilter)
  }


  def createDb = {
    getIdsPass // 1st pass
    persistIdMap
    createNodes // 2nd pass
    createRelationshipsPass // 3rd pass
    createPropertiesPass // 4th pass
    shutdown
  }

  def getIdsPass = {
    logger.info("starting stage (collecting machine ids)...")
    stage += 1
    val rdfIterable = getRdfIterable(freebaseFile)
    var count = 0l
    val start = System.currentTimeMillis
    rdfIterable.foreach {
      triple =>
        if (settings.nodeTypePredicates.contains(triple.predicateString)) {
          idMap.put(Utils.extractId(triple.objectString))
        }
        count = count + 1
        Utils.displayProgress(stage, "get machine ids", start, totalLines, "triples", count, idMap.length, "machine ids")
    }
    totalLines = count
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
    (0 until idMap.length).foreach {
      i =>
        batchInserter.createNode(i, Map[String, java.lang.Object]("mid" -> mid2long.decode(idMap.arr(i))).asJava, freebaseLabel)
        Utils.displayProgress(stage, "create nodes", start, idMap.length, "nodes", i, i, "nodes")
    }
    Utils.displayDone(stage, "create nodes", start, idMap.length, "nodes", idMap.length, "nodes")
    logger.info("done creating the nodes...")
  }

  trait Item {}

  case class GoodItem(subjectString: String, predicateString: String, objectString: String, count: Long = 0) extends Item

  case class PoisonItem(count: Long) extends Item

  class RelationshipWriter(start: Long, queue: ConcurrentRingBuffer[Item], doneSignal: CountDownLatch) extends Runnable {
    def run() {
      var relationshipCount = 0l

      while (true) {
        val item = queue.tryGet
        item match {
          case Some(e: GoodItem) => {
            val mid = Utils.extractId(e.subjectString)
            val nodeId: Long = idMap.get(mid)
            if (nodeId >= 0) {
              val objMid = Utils.extractId(e.objectString)
              val objNodeId: Long = idMap.get(objMid)
              if (objNodeId >= 0) {
                // create relationship
                batchInserter.createRelationship(nodeId, objNodeId, DynamicRelationshipType.withName(sanitize(e.predicateString)), null)
                relationshipCount += 1
                Utils.displayProgress(stage, "create relationships", start, totalLines, "triples", e.count, relationshipCount, "relationships")
              }
            }
          }
          case Some(e: PoisonItem) => {
            //println("relwriter dying, received poison")
            Utils.displayDone(stage, "create relationships", start, e.count, "triples", relationshipCount, "relationships")
            doneSignal.countDown
            return
          }
          case None => {
            // println("relwriter sleeping, buffer empty")
            Thread.sleep(10)
          }
          case _ => println("relwriter dying, unexpected relitem type"); doneSignal.countDown; return
        }
      }
    }
  }

  def createRelationshipsPass = {
    logger.info("starting create relationships pass...")
    stage += 1
    val start = System.currentTimeMillis
    val buffer = new ConcurrentRingBuffer[Item](1024 * 128)
    val doneSignal = new CountDownLatch(1)
    new Thread(new RelationshipWriter(start, buffer, doneSignal)).start()

    val rdfIterable = getRdfIterable(freebaseFile)
    var count = 0l
    rdfIterable.foreach {
      triple =>
      // if subject is an mid
        if (triple.subjectString.startsWith("<http://rdf.freebase.com/ns/m.")) {
          // if object is an mid (this is a relationship) and
          // if predicate isn't ignored
          if (triple.objectString.startsWith("<http://rdf.freebase.com/ns/m.") &&
            !settings.filters.predicateFilter.blacklist.equalsSeq.contains(triple.predicateString)) {
            while (!buffer.tryPut(GoodItem(triple.subjectString, triple.predicateString, triple.objectString, count))) {
              //println("relreader sleeping, buffer full")
              Thread.sleep(10)
            }
          }
        }
        count = count + 1
    }

    while (!buffer.tryPut(PoisonItem(count))) {
      //println("relreader sleeping, buffer full")
      Thread.sleep(10)
    }

    doneSignal.await()
    logger.info("done create relationships pass...")
  }

  class PropertyWriter(start: Long, queue: ConcurrentRingBuffer[Item], doneSignal: CountDownLatch) extends Runnable {
    def run() {
      var propertyCount = 0l
      while (true) {
        val item = queue.tryGet
        item match {
          case Some(e: GoodItem) => {
            val mid = Utils.extractId(e.subjectString)
            val nodeId: Long = idMap.get(mid)
            if (nodeId >= 0) {
              if (!settings.filters.predicateFilter.blacklist.equalsSeq.contains(e.predicateString) &&
                (settings.filters.predicateFilter.whitelist.equalsSeq.contains(e.predicateString) ||
                  !startsWithAny(e.predicateString, settings.filters.predicateFilter.blacklist.startsWithSeq)) &&
                (endsWithAny(e.objectString, settings.filters.objectFilter.whitelist.endsWithSeq) ||
                  startsWithAny(e.objectString, settings.filters.objectFilter.whitelist.startsWithSeq) ||
                  (!startsWithAny(e.objectString, settings.filters.objectFilter.blacklist.startsWithSeq) &&
                    !containsAny(e.objectString, settings.filters.objectFilter.blacklist.containsSeq)))) {
                val key = sanitize(e.predicateString)
                // if property exists, convert it to an array of properties
                // if it's already an array, append to the array
                if (batchInserter.nodeHasProperty(nodeId, key)) {
                  var prop = batchInserter.getNodeProperties(nodeId).get(key)
                  batchInserter.removeNodeProperty(nodeId, key)
                  prop match {
                    case prop: Array[String] => {
                      batchInserter.setNodeProperty(nodeId, key, prop :+ e.objectString)
                    }
                    case _ => {
                      batchInserter.setNodeProperty(nodeId, key, Array[String](prop.toString) :+ e.objectString)
                    }
                  }
                } else {
                  batchInserter.setNodeProperty(nodeId, key, e.objectString)
                }
                propertyCount += 1
                Utils.displayProgress(stage, "create properties", start, totalLines, "triples", e.count, propertyCount, "properties")
              }
            }
          }
          case Some(e: PoisonItem) => {
            //println("propwriter dying, received poison")
            Utils.displayDone(stage, "create properties", start, e.count, "triples", propertyCount, "properties")
            doneSignal.countDown
            return
          }
          case None => {
            // println("propwriter sleeping, buffer empty")
            Thread.sleep(100)
          }
          case _ => {
            // println("propwriter dying, unexpected propitem type")
            doneSignal.countDown;
            return
          }
        }
      }
    }
  }

  def createPropertiesPass = {
    logger.info("starting create properties pass...")
    stage += 1
    val start = System.currentTimeMillis
    val buffer = new ConcurrentRingBuffer[Item](1024 * 128)
    val doneSignal = new CountDownLatch(1)
    new Thread(new PropertyWriter(start, buffer, doneSignal)).start()

    val rdfIterable = getRdfIterable(freebaseFile)
    var count = 0l
    rdfIterable.foreach {
      triple =>
      // if subject is an mid
        if (triple.subjectString.startsWith("<http://rdf.freebase.com/ns/m.")) {
          if (!triple.objectString.startsWith("<http://rdf.freebase.com/ns/m.")) {
            // create property
            while (!buffer.tryPut(GoodItem(triple.subjectString, triple.predicateString, triple.objectString, count))) {
              // println("propreader sleeping, buffer full")
              Thread.sleep(10)
            }
          }
        } else {
          // TODO handle labels?
        }
        count = count + 1
    }

    while (!buffer.tryPut(PoisonItem(count))) {
      // println("propreader sleeping, buffer full")
      Thread.sleep(10)
    }

    doneSignal.await()
    logger.info("done create properties pass...")
  }

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