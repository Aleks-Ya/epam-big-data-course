package lesson3.spark

import lesson3.ipinfo.{IpInfo, LongSizeBoundedList}
import lesson3.net.TcpPacket
import lesson3.settings.service.DefaultIpSettings
import org.scalatest.{FlatSpec, Matchers}

class TrafficAnalyzerStepsTest extends FlatSpec with Matchers {

  it should "mapIpToPacket" in {
    val ip = "123.456.294.111"
    val packet = new TcpPacket(ip, 3000)
    val value = TrafficAnalyzerSteps.mapIpToPacket(packet)
    value shouldEqual ((ip, packet))
  }

  it should "reducePacketSize" in {
    val size1 = 3000
    val size2 = 5000
    val ip = "123.456.294.111"
    val value = TrafficAnalyzerSteps.reducePacketSize(
      new TcpPacket(ip, size1),
      new TcpPacket(ip, size2)
    )
    value shouldEqual new TcpPacket(ip, size1 + size2)
  }

  it should "addSettings" in {
    val ip = "123.456.294.111"
    val packet = new TcpPacket(ip, 3000)
    val value = TrafficAnalyzerSteps.addSettings(ip, packet)
    value shouldEqual(ip, (packet, DefaultIpSettings))
  }

  it should "updateIpInfo" in {
    val ip = "123.456.294.111"
    val packet = new TcpPacket(ip, 3000)
    val ipSettings = DefaultIpSettings
    val history = new LongSizeBoundedList(3)
    val ipInfoOpt = Some(new IpInfo(ip, history, false, false))
    val newIpInfoOpt = TrafficAnalyzerSteps.updateIpInfo(Seq((packet, ipSettings)), ipInfoOpt)
    newIpInfoOpt shouldBe defined
    //TODO assert it
  }
}
