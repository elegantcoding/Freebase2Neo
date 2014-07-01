package freebase2neo


////match (n:freebase) return n
//
////https://groups.google.com/forum/#!msg/freebase-discuss/AG5sl7K5KBE/iR7p-YfTNsUJ
////-XX:CMSInitiatingOccupancyFraction=<percent>
//http://docs.neo4j.org/chunked/stable/configuration-io-examples.html#configuration-batchinsert


import collection.JavaConverters._

import java.io.{BufferedReader, InputStreamReader, FileInputStream}

import java.util.zip.GZIPInputStream

import gnu.trove.map.hash.TObjectLongHashMap

import org.neo4j.graphdb.{DynamicRelationshipType, DynamicLabel}
import org.neo4j.unsafe.batchinsert.BatchInserters

import rdfProcessor.rdfProcessor.{RdfLineProcessor, CleanerFunction}

import rdfProcessor.{RdfProcessor, RdfFileProcessor, RfdCleaner}

import rdftriple.{ValidRdfTriple}
import rdftriple.rdftriple.RdfTriple


abstract class NeoRfdCleaner extends RfdCleaner {

    override val subjectCleaner = None
    override val predicateCleaner = None
    override val objectCleaner = None

    import NeoRfdCleaner._

    val sanitize : CleanerFunction =  Option((string : String) => {

        sanitizeRegex.replaceAllIn(string, "_")
    })

}

object NeoRfdCleaner {

    val sanitizeRegex = """[^A-Za-z0-9]""".r

}

object processForIdsRdfCleaner extends NeoRfdCleaner {
    override val subjectCleaner = sanitize
    override val predicateCleaner = None
    override val objectCleaner = None
}

object processBuildRelationshipsRdfCleaner extends NeoRfdCleaner {
    override val subjectCleaner = None
    override val predicateCleaner = sanitize
    override val objectCleaner = None
}


abstract class NeoRdfFileProcessor extends RdfFileProcessor {

    //override val rfdCleaner = emptyRdfCleaner

    override def validateRdfTriple(subject: String, predicate: String, obj: String) : RdfTriple = {
        ValidRdfTriple(subject, predicate, obj)
    }
}

object freebase2NeoProcessor extends RdfProcessor {

    val neoFlag = true
    val inserter = BatchInserters.inserter(Settings.outputGraphPath);

    val idMap = new TObjectLongHashMap[String]()

    var instanceCount = 0L


    override def init : Unit = {}

    override def shutdown : Unit = {
        inserter.shutdown();
//        errorLogFileWriter.close
//
//        statusLogFileWriter match {
//            case Some(fileWriter) => fileWriter.close
//            case _ => {}
//        }

    }

    def getProcessFiles = {

        Seq(
            processForIds,
            processBuildRelationships
        )
    }

    def getRdfStream : BufferedReader = {

        new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(Settings.gzippedTurtleFile))))

    }

    def handleInvalidTriple(rdfTriple : RdfTriple, tripleString : String) = {}

    object processForIds extends RdfFileProcessor {

        override val rfdCleaner = processForIdsRdfCleaner

        val processName = "processForIds"

        val rdfLineProcessor : RdfLineProcessor = (rdfTriple : RdfTriple, tripleString:String) => {

            //writeToStatusLog("setting label: " + tripleString)

            if(!idMap.contains(rdfTriple.objString)) {

                instanceCount += 1
                idMap.put(rdfTriple.objString, new java.lang.Long(instanceCount))

                if(neoFlag) {

                    inserter.createNode(instanceCount, Map[String,Object]("mid" -> rdfTriple.objString).asJava, DynamicLabel.label("freebase"))
                }
            }

            if(neoFlag) {

                var curLabels = inserter.getNodeLabels(instanceCount).asScala.toArray

                curLabels = curLabels :+ DynamicLabel.label(rdfTriple.subjectString)

                inserter.setNodeLabels(instanceCount, curLabels : _*) // the _* is for varargs
            }
        }
    }

    object processBuildRelationships extends RdfFileProcessor {

        override val rfdCleaner = processBuildRelationshipsRdfCleaner

        val processName = "processBuildRelationships"

        val rdfLineProcessor : RdfLineProcessor = (rdfTriple : RdfTriple, tripleString:String) => {

            if (idMap.contains(rdfTriple.subjectString)) {
                // this is a property/relationship of a node
                val subjectId = idMap.get(rdfTriple.subjectString)

                if(idMap.contains(rdfTriple.objString)) {
                    // this is a relationship
                    //writeToStatusLog("creating relationship: " + tripleString)
                    val objId = idMap.get(rdfTriple.objString)

                    if(neoFlag) {

                        inserter.createRelationship(subjectId, objId, DynamicRelationshipType.withName(rdfTriple.predicateString), null)
                    }

                } else {
                    // this is a real property
                    //writeToStatusLog("setting property: " + tripleString)

                    if (rdfTriple.objString.startsWith("<http://rdf.freebase.com/ns/m.")) {
                        //logRdfError(MissingIdRdfError("dropping relationship on the ground for an id we don't have:"), tripleString, rdfTriple)

                    } else {

                        val trimmedObj = objectTrimmerRegex.replaceAllIn(rdfTriple.objString, "")

                        if((trimmedObj.length > 3 && trimmedObj.substring(trimmedObj.length-3)(0) != '.' || trimmedObj.endsWith(".en")) &&
                           (rdfTriple.predicateString.length > 3 && rdfTriple.predicateString.substring(rdfTriple.predicateString.length-3)(0) != '_' || rdfTriple.predicateString.endsWith("_en"))) {

                            if(neoFlag) {

                                if(inserter.nodeHasProperty(subjectId, rdfTriple.predicateString)) {
                                    //logRdfError(PropertyExistsRdfError("already has prop: " + subjectId + "; rdfTriple.predicate: " + rdfTriple.predicateString), tripleString, rdfTriple)

                                    var prop = inserter.getNodeProperties(subjectId).get(rdfTriple.predicateString)
                                    inserter.removeNodeProperty(subjectId, rdfTriple.predicateString)
                                    //writeToStatusLog("got node property: " + subjectId + ":" + rdfTriple.predicateString + "; prop: " + prop)
                                    prop match {
                                        case prop:Array[String] => {
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

}

//import java.io.StringWriter
//import java.io.PrintWriter
//
//import java.lang.RuntimeException
//
//import java.io.{BufferedWriter, OutputStreamWriter, FileOutputStream}
//
//
//
////abstract class RdfError(errorString: String)
////case class MalformedRdfError(errorString: String) extends RdfError(errorString)
////case class MissingIdRdfError(errorString: String) extends RdfError(errorString)
////case class PropertyExistsRdfError(errorString: String) extends RdfError(errorString)
//
//case class MissingIdRdfError(errorString: String) extends InvalidRdfTripleReason(errorString)
//case class PropertyExistsRdfError(errorString: String) extends InvalidRdfTripleReason(errorString)
//
//
//object Main extends App {
//
//    val errorLogFileWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(Settings.errorLogFile)))
//    val statusLogFileWriter = if(Settings.statusLogFile.isEmpty) None else Some(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(Settings.statusLogFile))))
//
//    val objectTrimmerRegex = """^\"|\"$""".r
//    val sanitizeRegex = """[^A-Za-z0-9]""".r
//
//    val ALL = -1L
//    val ONE_MILLION = 1000000L
//    val ONE_BILLION = 1000000000L
//
//    val linesToProcess = ONE_MILLION * 50L
//
//    var rdfLineCount = 0L
//    var instanceCount = 0L
//    val startTime = System.currentTimeMillis
//    var lastTime = System.currentTimeMillis
//    val idMap = new TObjectLongHashMap[String]()
//
//
//    def writeToStatusLog(string : String) = {
//
//        statusLogFileWriter match {
//            case Some(fileWriter) => fileWriter.write(string + "\r\n")
//            case _ => {}
//        }
//    }
//
//    def trim(string:String) : String = {
//
//        val length = string.length()
//
//        if (string.startsWith("\"") && string.contains("\"@")) {
//            // "Droge"@de
//            if (string.endsWith("\"@en")) {
//                string.substring(1, length - 4)
//            }
//            else {
//                ""
//            } // only care about English for now
//        } else if (string.contains("/key/")) {
//            // keys are not human readable and we throw it away
//            ""
//        } else if (string.startsWith(Settings.fbRdfPrefix) && string.endsWith(">")) {
//            // <http://rdf.freebase.com/ns/g.1254x65_q>
//            string.substring(Settings.fbRdfPrefixLen , length - 1)
//        } else if (string.startsWith("\"") && string.endsWith("\"")) {
//            string.substring(1, length - 1)
//        } else if (string.startsWith("<") && string.endsWith(">")) {
//            string.substring(1, length - 1)
//        } else if (string.startsWith("\"") && string.contains("\"^^")) {
//            // "1987-06-17"^^<http://www.w3.org/2001/XMLSchema#date>
//            string.substring(1, string.indexOf("\"^^"))
//        } else {
//            string
//        }
//    }
//
//    def printStatus(processName : String, processStartTime : Long) = {
//
//        if((linesToProcess != ALL) && (rdfLineCount >= linesToProcess)) {
//
//            throw new RuntimeException
//        }
//
//        rdfLineCount += 1
//
//        if(rdfLineCount % (ONE_MILLION * 10L) == 0) {
//
//            val curTime = System.currentTimeMillis
//
//            println(processName + ": " + rdfLineCount/1000000 + "M tripleString lines processed" +
//                    "; last 10M: " + elapsedTimeToString(curTime - lastTime) +
//                    "; process elapsed: " + elapsedTimeToString(curTime - processStartTime) +
//                    " total elapsed: " + elapsedTimeToString(curTime - startTime))
//
//            lastTime = curTime
//            println("idMap size: " + idMap.size)
//        }
//    }
//
//    def logRdfError(rdfError : InvalidRdfTripleReason, tripleString : String, rdfTuple : Any) = {
//
//        //rdfError match {
//        //
//        //    case MalformedRdfError(errorString) => {
//        //
//        //        rdfTuple match {
//        //            case (first : String, second : String, third : String) => { errorLogFileWriter.write(" tuple size : 3 <" + first + "> <" + second + "> <" + third + "> line:\r\n " + tripleString + "\r\n") }
//        //            case (first : String, second : String) => { errorLogFileWriter.write(" tuple size : 2 <" + first + "> <" + second + "> line:\r\n " + tripleString + "\r\n") }
//        //            case (first : String) => { errorLogFileWriter.write(" tuple size : 1 <" + first + "> line:\r\n " + tripleString + "\r\n") }
//        //            case () => { errorLogFileWriter.write(" empty tuple "  + " line:\r\n " + tripleString + "\r\n") }
//        //            case _ => { errorLogFileWriter.write(" tuple type : " + rdfTuple.getClass.getName  + " line:\r\n " + tripleString + "\r\n") }
//        //        }
//        //    }
//        //
//        //    case MissingIdRdfError(errorString) => {
//        //    }
//        //
//        //    case PropertyExistsRdfError(errorString) => {
//        //    }
//        //
//        //    case _ => {
//        //    }
//        //
//        //}
//    }
//
//    @inline def isValidTriple(subject : String, predicate : String, obj : String) = {
//
//        if(subject.isEmpty || predicate.isEmpty || obj.isEmpty)
//            false
//
//        if(subject.startsWith("@base") || subject.startsWith("@prefix") || subject.startsWith("#"))
//            false
//
//        true
//    }
//
//    @inline def isValidIdTriple(subject : String, predicate : String, obj : String) = {
//
//        //println("predicate : " + predicate)
//
//        //println("Settings.nodeTypePredicates.contains(predicate) : " + Settings.nodeTypePredicates.contains(predicate))
//
//        if(isValidTriple(subject, predicate, obj) &&
//           Settings.nodeTypePredicates.contains(predicate) &&
//           ((Settings.nodeTypeSubjects.isEmpty || stringContainsStrings(subject, Settings.nodeTypeSubjects)) ||
//            (Settings.nodeTypeSubjectsConjunctive.isEmpty || stringContainsStringConjunctive(subject, Settings.nodeTypeSubjectsConjunctive))))
//            true
//        else
//            false
//    }
//
//
//
//
//    @inline def sanitize(string : String) = {
//
//        sanitizeRegex.replaceAllIn(string, "_")
//    }
//
//
//
///*
//  def configureIndex(graphDb:GraphDatabaseService, l:Label, key:String):IndexDefinition = {
//    var indexDefinition:IndexDefinition = null
//    val tx = graphDb.beginTx();
//    try {
//      val indexDefinition = graphDb.schema.indexFor(l)
//        .on(key)
//        .create()
//      tx.success()
//    } finally {
//      tx.finish()
//    }
//    indexDefinition
//  }
//*/
//}
