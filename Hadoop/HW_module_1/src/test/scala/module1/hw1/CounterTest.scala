package module1.hw1

import org.scalatest.{FlatSpec, Matchers}

class CounterTest extends FlatSpec with Matchers {

  it should "create map id->count" in {
    val map = Counter.processFile(getClass.getResourceAsStream("bid.txt"))
    map should have size 2
    map should contain key "20130612000102824"
    map should contain key "20130612000102827"
    map("20130612000102824") shouldEqual 2
    map("20130612000102827") shouldEqual 1
  }

}

