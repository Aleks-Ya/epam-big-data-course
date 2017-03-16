package lesson3.spark

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

}
