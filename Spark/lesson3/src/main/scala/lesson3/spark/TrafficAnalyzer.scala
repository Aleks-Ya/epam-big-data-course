package lesson3.spark

import java.io.Serializable

import lesson3.ipinfo.{IpInfo, IpInfoHelper}
import lesson3.net.TcpPacket
import org.apache.spark.streaming.dstream.DStream

class TrafficAnalyzer(private val stream: DStream[TcpPacket])
  extends Serializable {

  stream
    .map(packet => (packet.ip, packet))
    .reduceByKey((p1, p2) => new TcpPacket(p1.ip, p1.size + p2.size))
    .map(pair => {
      val ip = pair._1
      val packet = pair._2
      val settings = TrafficAnalyzerHelper.settingsByIp(ip)
      (ip, (packet, settings))
    })
    .updateStateByKey((pairs, ipInfoOpt: Option[IpInfo]) => {
      assert(pairs.size <= 1)
      if (pairs.nonEmpty) {
        val pair = pairs.head
        val packet = pair._1
        val settings = pair._2
        val ip = packet.ip
        val ipInfo = ipInfoOpt.getOrElse(IpInfoHelper.newIpInfo(ip, settings))
        ipInfo.history.append(packet.size)
        TrafficAnalyzerHelper.processThreshold(ip, settings, ipInfo)
        TrafficAnalyzerHelper.processLimit(ip, settings, ipInfo)
        Some(ipInfo)
      } else {
        val ipInfo = ipInfoOpt.get
        ipInfo.history.append(0)
        Some(ipInfo)
      }
    })
    .print()
}


