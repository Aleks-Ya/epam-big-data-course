package module1.hw1

import java.util.concurrent.locks.ReentrantLock

import org.scalatest.{FlatSpec, Matchers}

class CounterTest extends FlatSpec with Matchers {

  it should "create map id->count" in {
    val joinedMap = collection.mutable.Map[Long, Int]()
    val lock = new ReentrantLock()
    val counter = new Counter(getClass.getResourceAsStream("counter.txt"), joinedMap, lock)
    counter.call()
    joinedMap should have size 2
    joinedMap should contain key 20130612000102824L
    joinedMap should contain key 20130612000102827L
    joinedMap(20130612000102824L) shouldEqual 2
    joinedMap(20130612000102827L) shouldEqual 1
  }

}

