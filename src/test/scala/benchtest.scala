package com.elegantcoding.freebase2neo.test

import grizzled.slf4j.Logger
import java.io.FileInputStream
import org.scalatest._
import com.elegantcoding.rdfprocessor.NTripleIterable
import java.util.zip.GZIPInputStream
import com.elegantcoding.freebase2neo._

class benchtest extends FlatSpec with ShouldMatchers {
  val logger = Logger("com.elegantcoding.freebase2neo")
  val idMap = new IdMap()

  "bench" should "be able to scan the file" in {
    // pass 1, get ids
    val nts = new NTripleIterable(new GZIPInputStream(new FileInputStream(Settings.gzippedNTripleFile)))
    var count = 0l
    val processStartCount = System.currentTimeMillis
    nts.foreach { triple =>
      if(triple.predicateString == "<http://rdf.freebase.com/ns/type.type.instance>") {
        val mid = extractId(triple.objectString)
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

  def extractId(str:String):Long = {
    mid2long.encode(str.substring(31, str.length()-1))
  }

  def cleanSubject(str:String) = {
    str.replaceAllLiterally(Settings.fbRdfPrefix, "")
  }

  def cleanPredicate(str:String) = {
    str.replaceAllLiterally(Settings.fbRdfPrefix, "")
  }

  def cleanObject(str:String) = {
    var str2 = str.replaceAllLiterally(Settings.fbRdfPrefix, "")
    if(str2.endsWith(">")){
      str2.substring(0,str2.length()-1)
    } else {
      str
    }
  }

}
