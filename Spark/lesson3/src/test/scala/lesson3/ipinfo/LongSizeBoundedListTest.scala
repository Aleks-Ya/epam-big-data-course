package lesson3.ipinfo

import org.scalatest.{FlatSpec, Matchers}

class LongSizeBoundedListTest extends FlatSpec with Matchers {

  it should "recalculate sum of elements" in {
    val list = new LongSizeBoundedList(3)
    assertSizeAndSum(list, 0, 0)

    list.append(1)
    assertSizeAndSum(list, 1, 1)

    list.append(2)
    assertSizeAndSum(list, 2, 3)

    list.append(3)
    assertSizeAndSum(list, 3, 6)

    list.append(4)
    assertSizeAndSum(list, 3, 9)
  }

  private def assertSizeAndSum(list: LongSizeBoundedList, size: Long, sum: Long): Unit = {
    list.sum shouldEqual sum
    list.size shouldEqual size
  }
}
