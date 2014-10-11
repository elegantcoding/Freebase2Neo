import com.elegantcoding.freebase2neo.{mid2long}

import org.scalatest._
import scala.util.Random

class mid2longSpec extends FlatSpec with ShouldMatchers {

  "mid2long" should "be able to encode and decode mids" in {
    for (x <- 0 to 1000) {
      val r = new Random(System.currentTimeMillis())
      val l = math.abs(r.nextLong() % (32^12-1))
      val str = mid2long.decode(l)
      l should be(mid2long.encode(str))
    }
  }
}
