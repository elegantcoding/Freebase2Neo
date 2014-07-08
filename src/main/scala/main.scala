package com.elegantcoding.freebase2neo

import com.elegantcoding.rdfprocessor.NTripleIterable
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import grizzled.slf4j.Logger

object Main extends App {
  val logger = Logger("com.elegantcoding.freebase2neo")
  val idMap = new IdMap()

  // pass 1, get ids
  val nts = new NTripleIterable(new GZIPInputStream(new FileInputStream(Settings.gzippedNTripleFile)))
  var count = 0l
  val processStartCount = System.currentTimeMillis
  nts.foreach { triple =>
    if(triple.predicateString == "<http://rdf.freebase.com/ns/type.type.instance>") {
      val mid = Utils.extractId(triple.objectString)
      idMap.put(mid)
    }
    count = count + 1
    Utils.logStatus(processStartCount, count)
  }
  val start = System.currentTimeMillis()/1000
  logger.info("done reading file...")
  logger.info("idmap length: " + idMap.length)
  logger.info("sorting idMap...")
  idMap.done
  logger.info("done sorting/deduping... in "+(System.currentTimeMillis()-start)/1000)
  logger.info("idmap length: " + idMap.length)
  // TODO pass 2
}
