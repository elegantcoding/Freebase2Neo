package com.elegantcoding.freebase2neo.test

import grizzled.slf4j.Logger
import java.io.FileInputStream
import org.scalatest._
import com.elegantcoding.rdfprocessor.NTripleStream
import com.elegantcoding.rdfprocessor.rdftriple.ValidRdfTriple
import java.util.zip.GZIPInputStream
import com.elegantcoding.freebase2neo._

class benchtest extends FlatSpec with ShouldMatchers {
  val logger = Logger("com.elegantcoding.freebase2neo")

  "bench" should "be able to scan the file" in {
    val nts = new NTripleStream(new GZIPInputStream(new FileInputStream(Settings.gzippedNTripleFile)))
    var count = 0l
    val processStartCount = System.currentTimeMillis
    nts.stream.map { triple =>
      ValidRdfTriple(
        cleanSubject(triple.predicateString),
        cleanPredicate(triple.predicateString),
        cleanObject(triple.objectString))
    }.foreach{ triple =>
          count = count + 1
          Utils.logStatus(processStartCount, count)
    }
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
