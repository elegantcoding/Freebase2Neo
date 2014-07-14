import com.elegantcoding.freebase2neo.{IdMap, Main}

import grizzled.slf4j.Logger
import org.scalatest._

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

}
