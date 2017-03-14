package lesson3.spark

import lesson3.Context
import lesson3.incident.IncidentHelper
import lesson3.ipinfo.IpInfo
import lesson3.settings.IpSettings
import org.slf4j.LoggerFactory

object TrafficAnalyzerHelper extends Serializable {
  private val log = LoggerFactory.getLogger(getClass)

  def settingsByIp(ip: String): IpSettings = {
    Context.settingsService.getIpSettings(ip)
  }

  def processThreshold(ip: String, settings: IpSettings, newIpInfo: IpInfo): Unit = {
    val incidentOpt = IncidentHelper.isThresholdExceed(ip, newIpInfo, settings)
    if (incidentOpt.nonEmpty) {
      Context.kafkaService.sendEvent(incidentOpt.get)
      newIpInfo.thresholdExceed = true
    }
  }

  def processLimit(ip: String, settings: IpSettings, newIpInfo: IpInfo): Unit = {
    val incidentOpt = IncidentHelper.isLimitExceed(ip, newIpInfo, settings)
    if (incidentOpt.nonEmpty) {
      Context.kafkaService.sendEvent(incidentOpt.get)
      newIpInfo.limitExceed = true
    }
  }

  def logDebug(message: String): Unit = {
    log.debug(message)
  }
}
