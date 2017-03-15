package lesson3.ipinfo

import org.scalatest.{FlatSpec, Matchers}

class IpInfoCalculatorTest extends FlatSpec with Matchers {

  it should "calculate Downloaded Total for the period" in {
    val ipInfo = makeIpInfoWithHistory(3, 2, 4, 6)
    IpInfoCalculator.calculateDownloadedTotal(ipInfo, 2) shouldEqual 10
  }

  it should "calculate Downloaded Total for period longer than history" in {
    val ipInfo = makeIpInfoWithHistory(3, 2, 4, 6)
    IpInfoCalculator.calculateDownloadedTotal(ipInfo, 4) shouldEqual 12
  }

  it should "calculate Downloaded Total for zero period" in {
    val ipInfo = makeIpInfoWithHistory(3, 2, 4, 6)
    IpInfoCalculator.calculateDownloadedTotal(ipInfo, 0) shouldEqual 0
  }

  it should "calculate Downloaded Total for negative period" in {
    assertThrows[IllegalArgumentException] {
      IpInfoCalculator.calculateDownloadedTotal(makeIpInfoWithHistory(0), -1)
    }
  }

  it should "calculate Downloaded Total for history contains empty element" in {
    val ipInfo = makeIpInfoWithHistory(5, 2, 4)
    IpInfoCalculator.calculateDownloadedTotal(ipInfo, 3) shouldEqual 6
  }

  it should "calculate Download Rate for the period" in {
    val ipInfo = makeIpInfoWithHistory(3, 2, 4, 6)
    IpInfoCalculator.calculateDownloadRate(ipInfo, 2) shouldEqual 10 / 2
  }

  it should "calculate Download Rate for zero period" in {
    val ipInfo = makeIpInfoWithHistory(3, 2, 4, 6)
    IpInfoCalculator.calculateDownloadRate(ipInfo, 0) shouldEqual 0
  }

  it should "calculate Downloaded Rate for negative period" in {
    assertThrows[IllegalArgumentException] {
      IpInfoCalculator.calculateDownloadRate(makeIpInfoWithHistory(0), -1)
    }
  }

  it should "calculate Downloaded Rate for period longer than history" in {
    val ipInfo = makeIpInfoWithHistory(3, 2, 4, 6)
    IpInfoCalculator.calculateDownloadRate(ipInfo, 4) shouldEqual 4
  }

  private def makeIpInfoWithHistory(size: Long, elements: Long*): IpInfo = {
    val history = new LongSizeBoundedList(size)
    elements.foreach(history.append)
    new IpInfo("ip", history, false, false)
  }
}
