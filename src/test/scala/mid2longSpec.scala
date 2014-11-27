import com.elegantcoding.freebase2neo.{base32Converter}

import org.scalatest._
import scala.util.Random

class mid2longSpec extends FlatSpec with ShouldMatchers {

  "mid2long" should "be able to encode and decode mids" in {
    for (x <- 0 to 1000) {
      val r = new Random(System.currentTimeMillis())
      val l = math.abs(r.nextLong() % (32^12-1))
      val str = base32Converter.toBase32(l)
      l should be(base32Converter.toDecimal(str))
    }
  }
}
