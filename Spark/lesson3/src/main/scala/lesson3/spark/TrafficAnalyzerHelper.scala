package lesson3.spark

import lesson3.Context
import lesson3.event.{EventHelper, EventImpl, EventType}
import lesson3.ipinfo.IpInfo
import lesson3.settings.IpSettings

object TrafficAnalyzerHelper extends Serializable {
  def processThreshold(ip: String, settings: IpSettings, newIpInfo: IpInfo): Unit = {
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

  def processLimit(ip: String, settings: IpSettings, newIpInfo: IpInfo): Unit = {
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