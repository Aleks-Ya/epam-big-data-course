package lesson3.spark

import lesson3.Context
import lesson3.incident.{IncidentHelper, IncidentImpl, IncidentType}
import lesson3.ipinfo.IpInfo
import lesson3.settings.IpSettings

object TrafficAnalyzerHelper extends Serializable {

  def settingsByIp(ip: String): IpSettings = {
    Context.settingsService.getIpSettings(ip)
  }

  def processThreshold(ip: String, settings: IpSettings, newIpInfo: IpInfo): Unit = {
    val isThresholdExceed = IncidentHelper.isThresholdExceed(newIpInfo, settings)
    if (isThresholdExceed) {
      if (!newIpInfo.thresholdExceed) {
        val event = new IncidentImpl(ip, IncidentType.ThresholdExceed)
        Context.kafkaService.sendEvent(event)
        newIpInfo.thresholdExceed = true
      }
    } else {
      if (newIpInfo.thresholdExceed) {
        val event = new IncidentImpl(ip, IncidentType.ThresholdNorm)
        Context.kafkaService.sendEvent(event)
        newIpInfo.thresholdExceed = false
      }
    }
  }

  def processLimit(ip: String, settings: IpSettings, newIpInfo: IpInfo): Unit = {
    val isLimitExceed = IncidentHelper.isLimitExceed(newIpInfo, settings)
    if (isLimitExceed) {
      if (!newIpInfo.limitExceed) {
        val event = new IncidentImpl(ip, IncidentType.LimitExceed)
        Context.kafkaService.sendEvent(event)
        newIpInfo.limitExceed = true
      }
    } else {
      if (newIpInfo.thresholdExceed) {
        val event = new IncidentImpl(ip, IncidentType.LimitExceed)
        Context.kafkaService.sendEvent(event)
        newIpInfo.limitExceed = false
      }
    }
  }
}
