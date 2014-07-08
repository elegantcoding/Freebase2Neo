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

  getIdsFirstPass
  buildRelationshipsSecondPass

  inserter.shutdown
  logger.info("done!")

  def getIdsFirstPass = {
    logger.info("starting first pass (collecting machine ids)...")
    val nts = new NTripleIterable(new GZIPInputStream(new FileInputStream(Settings.gzippedNTripleFile), 65536*16))
    var count = 0l
    val processStartCount = System.currentTimeMillis
    nts.foreach { triple =>
      if (triple.predicateString == "<http://rdf.freebase.com/ns/type.type.instance>") {
        val mid = Utils.extractId(triple.objectString)
        idMap.put(mid)
      }
      count = count + 1
      Utils.logFirstPass(processStartCount, count, idMap.length)
    }
    val start = System.currentTimeMillis()/1000
    logger.info("sorting idMap...")
    idMap.done
    logger.info("done sorting/deduping... in "+(System.currentTimeMillis()-start)/1000)
    logger.info("final idmap length: " + idMap.length)
    Utils.logFirstPassDone(processStartCount, count, idMap.length)
  }

  def buildRelationshipsSecondPass = {
    logger.info("starting second pass...")
    val nts = new NTripleIterable(new GZIPInputStream(new FileInputStream(Settings.gzippedNTripleFile), 65536*16))
    var count = 0l
    var rels = 0l
    var nodes = 0l
    val processStartCount = System.currentTimeMillis
    nts.foreach { triple =>
      // if subject is an mid
      if (triple.subjectString.startsWith("<http://rdf.freebase.com/ns/m.")) {
        val mid = Utils.extractId(triple.subjectString)
        val nodeId:Long = idMap.get(mid)
        if (!idMap.getCreated(nodeId)) {
          idMap.setCreated(mid)
          inserter.createNode(
            nodeId,
            Map[String,Object]("mid" -> mid2long.decode(mid)).asJava,
            freebaseLabel
          )
          nodes += 1
        }
        // if predicate isn't ignored
        if (!Settings.ignorePredicates.contains(triple.predicateString)) {
          // if object is an mid (this is a relationship)
          if (triple.objectString.startsWith("<http://rdf.freebase.com/ns/m.")) {
            val objMid = Utils.extractId(triple.objectString)
            val objNodeId: Long = idMap.get(objMid)
            if (!idMap.getCreated(objMid)) {
              idMap.setCreated(objMid)
              inserter.createNode(
                objNodeId,
                Map[String, Object]("mid" -> mid2long.decode(objMid)).asJava,
                freebaseLabel
              )
              nodes += 1
            }
            // create relationship
            inserter.createRelationship(nodeId, objNodeId, DynamicRelationshipType.withName(sanitize(triple.predicateString)), null)
            rels += 1
          } else {
            // TODO handle properties?
          }
        }
      } else {
        // TODO handle labels?
      }
      count = count + 1
      Utils.logSecondPass(processStartCount, count, nodes, rels)
    }
    logger.info("done second pass...")
    Utils.logSecondPassDone(processStartCount, count, nodes, rels)
  }

  def sanitize(s:String) = {
    s.replaceAllLiterally(Settings.fbRdfPrefix, "")
    s.substring(0,s.length-1)
  }
}

