package lesson3.spark

import lesson3.ipinfo.{IpInfo, IpInfoHelper}
import lesson3.net.TcpPacket
import lesson3.settings.IpSettings

object TrafficAnalyzerSteps {
  val mapIpToPacket: TcpPacket => ((String, TcpPacket)) = packet => (packet.ip, packet)

  val reducePacketSize: (TcpPacket, TcpPacket) => TcpPacket = (p1, p2) => {
    assert(p1.ip == p2.ip)
    new TcpPacket(p1.ip, p1.size + p2.size)
  }

  val addSettings: ((String, TcpPacket)) => (String, (TcpPacket, IpSettings)) = pair => {
    val ip = pair._1
    val packet = pair._2
    val settings = TrafficAnalyzerHelper.settingsByIp(ip)
    val tuple = (ip, (packet, settings))
    TrafficAnalyzerHelper.logInfo("Tuple created: " + tuple)
    tuple
  }

  val updateIpInfo: (Seq[(TcpPacket, IpSettings)], Option[IpInfo]) => Option[IpInfo] = (pairs, ipInfoOpt) => {
    TrafficAnalyzerHelper.logInfo("Process pairs: " + pairs)
    assert(pairs.size <= 1)
    var ipInfo: IpInfo = null
    if (pairs.nonEmpty) {
      val pair = pairs.head
      val packet = pair._1
      val settings = pair._2
      val ip = packet.ip
      ipInfo = ipInfoOpt.getOrElse(IpInfoHelper.newIpInfo(ip, settings))
      ipInfo.history.append(packet.size)
      TrafficAnalyzerHelper.processThreshold(ip, settings, ipInfo)
      TrafficAnalyzerHelper.processLimit(ip, settings, ipInfo)
    } else {
      ipInfo = ipInfoOpt.get
      ipInfo.history.append(0)
    }
    if (ipInfo.history.sum > 0) {
      Some(ipInfo)
    } else {
      TrafficAnalyzerHelper.logInfo("Remove state: " + ipInfo)
      None
    }
  }

}
