package module1.hw1

import java.util.concurrent.locks.ReentrantLock

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

class CounterTest extends FlatSpec with Matchers {

  it should "create map id->count" in {
    val joinedMap: mutable.Map[String, Int] = collection.mutable.Map[String, Int]()
    val lock = new ReentrantLock()
    val counter = new Counter(getClass.getResourceAsStream("counter.txt"), joinedMap, lock)
    counter.call()
    joinedMap should have size 2
    joinedMap should contain key "20130612000102824"
    joinedMap should contain key "20130612000102827"
    joinedMap("20130612000102824") shouldEqual 2
    joinedMap("20130612000102827") shouldEqual 1
  }

}

