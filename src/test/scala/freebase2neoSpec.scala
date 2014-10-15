import com.elegantcoding.freebase2neo.{Freebase2Neo, Utils, IdMap, Main}

import org.apache.commons.io.FileUtils
import org.neo4j.graphdb.DynamicLabel
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.unsafe.batchinsert.BatchInserters
import org.scalatest._
import collection.JavaConverters._
import java.io.File
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.slf4j.Logger

class freebase2neoSpec extends FlatSpec with ShouldMatchers {

  val dbpath = "target/testdb"

  def createInserter = BatchInserters.inserter(
    dbpath,
    Map[String,String](
      "neostore.nodestore.db.mapped_memory" -> "64M",
      "neostore.relationshipstore.db.mapped_memory" -> "128M",
      "neostore.propertystore.db.mapped_memory" -> "128M",
      "neostore.propertystore.db.strings" -> "128M"
    ).asJava
  )

  val freebase2neo = {
    FileUtils.deleteDirectory(new File(dbpath))
    new Freebase2Neo(createInserter)
  }

  "freebase2neo" should "be able extract an id" in {
    val obj = "<http://rdf.freebase.com/ns/m.05ljt>"
    Utils.extractId(obj) should be(182809)
  }

  "freebase2neo" should "be able to count the ids" in {
    freebase2neo.logger = Logger(LoggerFactory.getLogger("freebase2neo.mainSpec"))
    freebase2neo.countIdsPass("subset.ntriple.gz")
    freebase2neo.totalIds should be (21007)
    freebase2neo.totalLines should be (8731903)
    freebase2neo.shutdown
  }

  it should "be able to get the ids" in {
    freebase2neo.inserter = createInserter
    freebase2neo.idMap = new IdMap(21054)
    freebase2neo.getIdsPass("subset.ntriple.gz")
    freebase2neo.persistIdMap
    freebase2neo.idMap.getMid("05ljtx") should be (1431)
    freebase2neo.shutdown
  }

  it should "be able to create the nodes" in {
    FileUtils.deleteDirectory(new File(dbpath))
    freebase2neo.inserter = createInserter
    freebase2neo.freebaseLabel = DynamicLabel.label("Freebase")
    freebase2neo.createNodes
    freebase2neo.shutdown
    // confirm nodes are created (check one high and low)
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(dbpath)
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
    freebase2neo.inserter = createInserter
    freebase2neo.freebaseLabel = DynamicLabel.label("Freebase")
    freebase2neo.createRelationshipsPass("subset.ntriple.gz")
    freebase2neo.shutdown
    // confirm nodes are created (check one high and low)
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(dbpath)
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
    freebase2neo.inserter = createInserter
    freebase2neo.freebaseLabel = DynamicLabel.label("Freebase")
    freebase2neo.createPropertiesPass("subset.ntriple.gz")
    freebase2neo.shutdown
    // confirm nodes are created (check one high and low)
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(dbpath)
    val tx = db.beginTx
    try {
      val n = db.getNodeById(4l) // Apache HTTP Server
      //println("test: "+ n.getProperty("common.topic.description"))
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