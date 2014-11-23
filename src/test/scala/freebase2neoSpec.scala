import com.elegantcoding.freebase2neo._

import org.apache.commons.io.FileUtils
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.unsafe.batchinsert.BatchInserters
import org.scalatest._
import collection.JavaConverters._
import java.io.File
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.slf4j.Logger

class freebase2neoSpec extends FlatSpec with ShouldMatchers {

  var log = Logger(LoggerFactory.getLogger("freebase2neoSpec"))

  val settings = new Settings("freebase2neo.test.json")

  log.info("source: " + settings.gzippedNTripleFile)
  log.info("db: " + settings.outputGraphPath)

  log.info("settings.nodeStoreMappedMemory : " + settings.nodeStoreMappedMemory)
  log.info("settings.relationshipStoreMappedMemory : " + settings.relationshipStoreMappedMemory)
  log.info("settings.propertyStoreMappedMemory : " + settings.propertyStoreMappedMemory)
  log.info("settings.propertyStoreStrings : " + settings.propertyStoreStrings)

  def createInserter = BatchInserters.inserter(
    settings.outputGraphPath,
    Map[String,String](
      "neostore.nodestore.db.mapped_memory" ->         settings.nodeStoreMappedMemory,
      "neostore.relationshipstore.db.mapped_memory" -> settings.relationshipStoreMappedMemory,
      "neostore.propertystore.db.mapped_memory" ->     settings.propertyStoreMappedMemory,
      "neostore.propertystore.db.strings" ->           settings.propertyStoreStrings
    ).asJava
  )

  FileUtils.deleteDirectory(new File(settings.outputGraphPath))

  var freebase2neo = new Freebase2Neo(createInserter, settings)
  freebase2neo.freebaseFile = "subset.ntriple.gz"

  "freebase2neo" should "be able extract an id" in {
    val obj = "<http://rdf.freebase.com/ns/m.05ljt>"
    freebase2neo.extractId(obj) should be(182809)
  }

  it should "be able to get the ids" in {
    //freebase2neo.batchInserter = createInserter
    // freebase2neo.idMap = new IdMap(21054)

    freebase2neo.getIdsPass
    freebase2neo.persistIdMap
    freebase2neo.idMap.getMid("05ljtx") should be (1431)
    freebase2neo.shutdown
  }

  it should "be able to create the nodes" in {
    FileUtils.deleteDirectory(new File(settings.outputGraphPath))
    freebase2neo.batchInserter = createInserter
    freebase2neo.createNodes
    freebase2neo.shutdown
    // confirm nodes are created (check one high and low)
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(settings.outputGraphPath)
    val tx = db.beginTx
    try {
      val n = db.getNodeById(0l)
      val n2 = db.getNodeById(8000l)
      val apache = db.getNodeById(4l)
      apache.getProperty("mid") should be("_h2")
      tx.success
    } catch {
      case t:Throwable => fail(t)
    } finally {
      tx.close
    }
    db.shutdown
  }

  it should "be able to create the relationships" in {
    freebase2neo.batchInserter = createInserter
    freebase2neo.createRelationshipsPass
    freebase2neo.shutdown
    // confirm nodes are created (check one high and low)
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(settings.outputGraphPath)
    val tx = db.beginTx
    var found = false
    try {
      val n = db.getNodeById(4l) // Apache HTTP Server
      for(r <- n.getRelationships.asScala) {
        // is notable type Software
        if (r.getOtherNode(n).getId == 3278 && r.getType.name == "common.topic.notable_types") {
          found = true
        }
      }
      if(!found) {
        fail
      }
      tx.success
    } catch {
      case t:Throwable => fail(t)
    } finally {
      tx.close
    }
    db.shutdown
  }

  it should "be able to create properties" in {
    freebase2neo.batchInserter = createInserter
    freebase2neo.createPropertiesPass
    freebase2neo.shutdown
    // confirm nodes are created (check one high and low)
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(settings.outputGraphPath)
    val tx = db.beginTx
    try {
      val n = db.getNodeById(4l) // Apache HTTP Server
      //println("test: "+ n.getProperty("common.topic.description"))
      n.getProperty("type.object.name").asInstanceOf[String] should be("\"Apache HTTP Server\"@en")
      n.getProperty("common.topic.description").asInstanceOf[String].startsWith("\"The Apache HTTP Server, commonly referred to as Apache") should be(true)
      tx.success
    } catch {
      case t:Throwable => fail(t)
    } finally {
      tx.close
    }
    db.shutdown
  }

}
