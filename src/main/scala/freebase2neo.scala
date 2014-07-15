package com.elegantcoding.freebase2neo

////match (n:freebase) return n
//
////https://groups.google.com/forum/#!msg/freebase-discuss/AG5sl7K5KBE/iR7p-YfTNsUJ
////-XX:CMSInitiatingOccupancyFraction=<percent>
//http://docs.neo4j.org/chunked/stable/configuration-io-examples.html#configuration-batchinsert

import java.io.{BufferedReader, InputStreamReader, FileInputStream}
import java.util.zip.GZIPInputStream
import org.neo4j.graphdb.{DynamicRelationshipType, DynamicLabel}
import org.neo4j.unsafe.batchinsert.BatchInserters

import com.elegantcoding.rdfprocessor.types.{RdfLineProcessor, CleanerFunction}
import com.elegantcoding.rdfprocessor.rdftriple.ValidRdfTriple
import com.elegantcoding.rdfprocessor.{RdfFileProcessor, RdfCleaner}
import com.elegantcoding.rdfprocessor.rdftriple.types.RdfTriple

import collection.JavaConverters._

abstract class NeoRdfCleaner extends RdfCleaner {

  override val subjectCleaner:CleanerFunction = None
  override val predicateCleaner:CleanerFunction = None
  override val objectCleaner:CleanerFunction = None

  def sanitize = Option((string: String) => {
    NeoRdfCleaner.sanitizeRegex.replaceAllIn(string, "_")
  })

}

object NeoRdfCleaner {
  val sanitizeRegex = """[^A-Za-z0-9]""".r
  val objectTrimmerRegex = """^\"|\"$""".r
}

import NeoRdfCleaner._

object processForIdsRdfCleaner extends NeoRdfCleaner {
  override val subjectCleaner = sanitize
  override val predicateCleaner = None
  override val objectCleaner = None
}

object processBuildRelationshipsRdfCleaner extends NeoRdfCleaner {
  override val subjectCleaner = None
  override val predicateCleaner = sanitize
  override val objectCleaner = None
}

abstract class NeoRdfFileProcessor extends RdfFileProcessor {
  //override val rdfCleaner = emptyRdfCleaner

  override def validateRdfTriple(subject: String, predicate: String, obj: String): RdfTriple = {
    ValidRdfTriple(subject, predicate, obj)
  }
}

object freebase2NeoProcessor extends NeoRdfFileProcessor {

  val neoFlag = true
  val processName = "freebase2neo"
  val inserter = BatchInserters.inserter(Settings.outputGraphPath);

  val idMap = new IdMap()

  var instanceCount = 0L

  def getProcessFiles = {
    Seq(
      processForIds,
      processBuildRelationships
    )
  }

  def getRdfStream: BufferedReader = {
    new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(Settings.gzippedNTripleFile))))
  }

  def handleInvalidTriple(rdfTriple: RdfTriple, tripleString: String) = {}

  object processForIds extends RdfFileProcessor {

    def getRdfStream: BufferedReader = {
      new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(Settings.gzippedNTripleFile))))
    }

    //override val rdfCleaner = processForIdsRdfCleaner

    val processName = "processForIds"

    override val rdfLineProcessor:RdfLineProcessor = (rdfTriple: RdfTriple) => {

      //writeToStatusLog("setting label: " + tripleString)
      /*if(!idMap.containsMid(rdfTriple.objectString)) {
          idMap.putMid(rdfTriple.objectString)

          if(neoFlag) {
              inserter.createNode(instanceCount, Map[String,Object]("mid" -> rdfTriple.objectString).asJava, DynamicLabel.label("freebase"))
          }
      }*/

      /*if(neoFlag) {
          var curLabels = inserter.getNodeLabels(instanceCount).asScala.toArray
          curLabels = curLabels :+ DynamicLabel.label(rdfTriple.subjectString)
          inserter.setNodeLabels(instanceCount, curLabels : _*) // the _* is for varargs
      }*/
    }
  }


  object processBuildRelationships extends RdfFileProcessor {

    def getRdfStream: BufferedReader = {
      new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(Settings.gzippedNTripleFile))))
    }

    override val rdfCleaner:RdfCleaner = processBuildRelationshipsRdfCleaner

    val processName = "processBuildRelationships"

    val rdfLineProcessor:RdfLineProcessor = (rdfTriple: RdfTriple) => {

      if (idMap.containsMid(rdfTriple.subjectString)) {
        // this is a property/relationship of a node
        val subjectId = idMap.getMid(rdfTriple.subjectString)

        if (idMap.containsMid(rdfTriple.objectString)) {
          // this is a relationship
          //writeToStatusLog("creating relationship: " + tripleString)
          val objId = idMap.getMid(rdfTriple.objectString)

          if (neoFlag) {
            inserter.createRelationship(subjectId, objId, DynamicRelationshipType.withName(rdfTriple.predicateString), null)
          }

        } else {
          // this is a real property
          //writeToStatusLog("setting property: " + tripleString)

          if (rdfTriple.objectString.startsWith("<http://rdf.freebase.com/ns/m.")) {
            //logRdfError(MissingIdRdfError("dropping relationship on the ground for an id we don't have:"), tripleString, rdfTriple)

          } else {

            val trimmedObj = objectTrimmerRegex.replaceAllIn(rdfTriple.objectString, "")

            if ((trimmedObj.length > 3 && trimmedObj.substring(trimmedObj.length - 3)(0) != '.' || trimmedObj.endsWith(".en")) &&
              (rdfTriple.predicateString.length > 3 && rdfTriple.predicateString.substring(rdfTriple.predicateString.length - 3)(0) != '_' || rdfTriple.predicateString.endsWith("_en"))) {

              if (neoFlag) {

                if (inserter.nodeHasProperty(subjectId, rdfTriple.predicateString)) {
                  //logRdfError(PropertyExistsRdfError("already has prop: " + subjectId + "; rdfTriple.predicate: " + rdfTriple.predicateString), tripleString, rdfTriple)

                  var prop = inserter.getNodeProperties(subjectId).get(rdfTriple.predicateString)
                  inserter.removeNodeProperty(subjectId, rdfTriple.predicateString)
                  //writeToStatusLog("got node property: " + subjectId + ":" + rdfTriple.predicateString + "; prop: " + prop)
                  prop match {
                    case prop: Array[String] => {
                      //writeToStatusLog("prop array detected...");
                      inserter.setNodeProperty(subjectId, rdfTriple.predicateString, prop :+ trimmedObj)
                    }
                    case _ => {
                      //writeToStatusLog("converting prop to array...");
                      inserter.setNodeProperty(subjectId, rdfTriple.predicateString, Array[String](prop.toString) :+ trimmedObj)
                    }
                  }
                } else {
                  inserter.setNodeProperty(subjectId, rdfTriple.predicateString, trimmedObj)
                }
              }
            }
          }
        }
      }

    }
  }

  override val rdfLineProcessor: RdfLineProcessor = (triple:RdfTriple) => {}
}
