package lesson3.spark

import java.io.Serializable

import lesson3.Context
import lesson3.event.{EventHelper, EventImpl, EventType}
import lesson3.ipinfo.{IpInfo, IpInfoHelper}
import lesson3.net.TcpPacket
import lesson3.settings.IpSettings
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
          processThreshold(ip, settings, newIpInfo)
          processLimit(ip, settings, newIpInfo)
        })
        Some(newIpInfo)
      } else {
        ipInfoOpt
      }
    })
    .print()

  private def processThreshold(ip: String, settings: IpSettings, newIpInfo: IpInfo) = {
    val isThresholdExceed = EventHelper.isThresholdExceed(newIpInfo, settings)
    if (isThresholdExceed) {
      if (!newIpInfo.thresholdExceed) {
        val event = new EventImpl(ip, EventType.ThresholdExceed)
        Context.kafkaService.sendEvent(event)
        newIpInfo.thresholdExceed = true
      }
    } else {
      if (newIpInfo.thresholdExceed) {
        val event = new EventImpl(ip, EventType.ThresholdNorm)
        Context.kafkaService.sendEvent(event)
        newIpInfo.thresholdExceed = false
      }
    }
  }

  private def processLimit(ip: String, settings: IpSettings, newIpInfo: IpInfo) = {
    val isLimitExceed = EventHelper.isLimitExceed(newIpInfo, settings)
    if (isLimitExceed) {
      if (!newIpInfo.limitExceed) {
        val event = new EventImpl(ip, EventType.LimitExceed)
        Context.kafkaService.sendEvent(event)
        newIpInfo.limitExceed = true
      }
    } else {
      if (newIpInfo.thresholdExceed) {
        val event = new EventImpl(ip, EventType.LimitExceed)
        Context.kafkaService.sendEvent(event)
        newIpInfo.limitExceed = false
      }
    }
  }
}


