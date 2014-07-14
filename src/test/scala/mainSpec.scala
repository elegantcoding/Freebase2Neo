import com.elegantcoding.freebase2neo.{IdMap, Main}

import grizzled.slf4j.Logger
import org.apache.commons.io.FileUtils
import org.neo4j.graphdb.DynamicLabel
import org.neo4j.cypher.ExecutionEngine
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.unsafe.batchinsert.BatchInserters
import org.scalatest._
import collection.JavaConverters._
import java.io.File

class mainSpec extends FlatSpec with ShouldMatchers {

  "main" should "be able to count the ids" in {
    Main.logger = Logger("test")
    Main.countIdsPass("subset.ntriple.gz")
    Main.totalIds should be (21054)
    Main.totalLines should be (8764511)
  }

  it should "be able to get the ids" in {
    Main.idMap = new IdMap(21054)
    Main.logger = Logger("test")
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
    val db = (new GraphDatabaseFactory()).newEmbeddedDatabase(Main.dbpath)
    val tx = db.beginTx
    try {
      val n = db.getNodeById(0l)
      val n2 = db.getNodeById(8000l)
      tx.success
    } catch {
      case t:Throwable => fail(t)
    } finally {
      tx.close
    }
    db.shutdown
  }

}
