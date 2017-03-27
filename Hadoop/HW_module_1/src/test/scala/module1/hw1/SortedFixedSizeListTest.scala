package module1.hw1

import org.scalatest.{FlatSpec, Matchers}

class SortedFixedSizeListTest extends FlatSpec with Matchers {

  it should "parse a data file's line" in {
    val list = new SortedFixedSizeList(3)

    val a = (10L, 1)
    val b = (11L, 2)
    val c = (12L, 3)
    val d = (13L, 3)
    val e = (14L, 4)

    list.add(d)
    list.add(e)
    list.add(a)
    list.add(c)
    list.add(b)

    list.toList should contain inOrderOnly(e, d, c)
  }
}

