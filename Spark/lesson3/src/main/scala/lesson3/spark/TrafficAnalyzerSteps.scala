package lesson3.spark

import lesson3.net.TcpPacket

object TrafficAnalyzerSteps {
  val mapIpToPacket = (packet: TcpPacket) => (packet.ip, packet)

  val reducePacketSize = (p1: TcpPacket, p2: TcpPacket) => {
    assert(p1.ip == p2.ip)
    new TcpPacket(p1.ip, p1.size + p2.size)
  }

  val addSettings = (pair: (String, TcpPacket)) => {
    val ip = pair._1
    val packet = pair._2
    val settings = TrafficAnalyzerHelper.settingsByIp(ip)
    val tuple = (ip, (packet, settings))
    TrafficAnalyzerHelper.logInfo("Tuple created: " + tuple)
    tuple
  }

}
