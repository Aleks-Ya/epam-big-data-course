package module1.hw1

import org.scalatest.{FlatSpec, Matchers}

class SortedFixedSizeListTest extends FlatSpec with Matchers {

  it should "parse a data file's line" in {
    val list = new SortedFixedSizeList(3)

    val a = ("a", 1)
    val b = ("b", 2)
    val c = ("c", 3)
    val d = ("d", 3)
    val e = ("e", 4)

    list.add(d)
    list.add(e)
    list.add(a)
    list.add(c)
    list.add(b)

    list.toList should contain inOrderOnly(e, d, c)
  }
}

