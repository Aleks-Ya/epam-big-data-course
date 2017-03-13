package lesson3

import java.io.Serializable

import lesson3.event.EventHelper
import lesson3.net.TcpPacket
import org.apache.spark.streaming.dstream.DStream

class TrafficAnalyzer(private val stream: DStream[TcpPacket])
  extends Serializable {

  stream
    .map(packet => (packet.ip, packet))
    .reduceByKey((p1, p2) => new TcpPacket(p1.ip, p1.size + p2.size, p1.settings))
    .updateStateByKey((newPackets, ipInfoOpt: Option[IpInfo]) => {
      if (newPackets.nonEmpty) {
        val ip = newPackets.head.ip
        val settings = newPackets.head.settings
        val ipInfo = ipInfoOpt.getOrElse(IpInfoHelper.newIpInfo(settings))
        val newIpInfo = ipInfo
        newPackets.foreach(packet => {
          newIpInfo.history.append(packet.size)
          EventHelper.checkThreshold(ip, newIpInfo, settings)
          EventHelper.checkLimit(ip, newIpInfo, settings)
        })
        Some(newIpInfo)
      } else {
        ipInfoOpt
      }
    })
    .print()

}


