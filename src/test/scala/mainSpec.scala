import com.elegantcoding.freebase2neo.{Utils, IdMap, Main}

import org.apache.commons.io.FileUtils
import org.neo4j.graphdb.DynamicLabel
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.unsafe.batchinsert.BatchInserters
import org.scalatest._
import collection.JavaConverters._
import java.io.File
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.slf4j.Logger

class mainSpec extends FlatSpec with ShouldMatchers {

  "main" should "be able to count the ids" in {
    Main.logger = Logger(LoggerFactory.getLogger("freebase2neo.mainSpec"))
    Main.countIdsPass("subset.ntriple.gz")
    Main.totalIds should be (21007)
    Main.totalLines should be (8731903)
  }

  it should "be able extract an id" in {
    val obj = "<http://rdf.freebase.com/ns/m.05ljt>"
    Utils.extractId(obj) should be(182809)
  }

  it should "be able to get the ids" in {
    Main.idMap = new IdMap(21054)
    Main.getIdsPass("subset.ntriple.gz")
    Main.persistIdMap
    Main.idMap.getMid("05ljtx") should be (1431)
  }

  it should "be able to create the nodes" in {
    Main.dbpath = "target/testdb"
    FileUtils.deleteDirectory(new File(Main.dbpath))
    Main.inserter = BatchInserters.inserter(
      Main.dbpath,
      Map[String,String](
        "neostore.nodestore.db.mapped_memory" -> "64M",
        "neostore.relationshipstore.db.mapped_memory" -> "128M",
        "neostore.propertystore.db.mapped_memory" -> "128M",
        "neostore.propertystore.db.strings" -> "128M"
      ).asJava
    )
    Main.freebaseLabel = DynamicLabel.label("Freebase")
    Main.createNodes
    Main.inserter.shutdown
    // confirm nodes are created (check one high and low)
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(Main.dbpath)
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
    Main.dbpath = "target/testdb"
    //FileUtils.deleteDirectory(new File(Main.dbpath))
    Main.inserter = BatchInserters.inserter(
      Main.dbpath,
      Map[String,String](
        "neostore.nodestore.db.mapped_memory" -> "64M",
        "neostore.relationshipstore.db.mapped_memory" -> "128M",
        "neostore.propertystore.db.mapped_memory" -> "128M",
        "neostore.propertystore.db.strings" -> "128M"
      ).asJava
    )
    Main.freebaseLabel = DynamicLabel.label("Freebase")
    Main.createRelationshipsPass("subset.ntriple.gz")
    Main.inserter.shutdown
    // confirm nodes are created (check one high and low)
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(Main.dbpath)
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
    Main.dbpath = "target/testdb"
    Main.inserter = BatchInserters.inserter(
      Main.dbpath,
      Map[String,String](
        "neostore.nodestore.db.mapped_memory" -> "64M",
        "neostore.relationshipstore.db.mapped_memory" -> "128M",
        "neostore.propertystore.db.mapped_memory" -> "128M",
        "neostore.propertystore.db.strings" -> "128M"
      ).asJava
    )
    Main.freebaseLabel = DynamicLabel.label("Freebase")
    Main.createPropertiesPass("subset.ntriple.gz")
    Main.inserter.shutdown
    // confirm nodes are created (check one high and low)
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(Main.dbpath)
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
