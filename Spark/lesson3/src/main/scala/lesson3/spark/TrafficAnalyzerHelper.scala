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

  def processThreshold(ip: String, settings: IpSettings, ipInfo: IpInfo): Unit = {
    val incidentOpt = IncidentHelper.createThresholdExceedIncident(ip, ipInfo, settings)
    if (incidentOpt.nonEmpty) {
      Context.kafkaService.sendEvent(incidentOpt.get)
      ipInfo.thresholdExceed = true
    }
  }

  def processLimit(ip: String, settings: IpSettings, ipInfo: IpInfo): Unit = {
    val incidentOpt = IncidentHelper.createLimitExceedIncident(ip, ipInfo, settings)
    if (incidentOpt.nonEmpty) {
      Context.kafkaService.sendEvent(incidentOpt.get)
      ipInfo.limitExceed = true
    }
  }

  def processHourStatistics(ip: String, ipInfo: IpInfo): Unit = {
    val statistics = IncidentHelper.newIpStatistics(ip, ipInfo)
    Context.hiveService.updateHourStatistics(statistics)
  }

  def logDebug(message: String): Unit = {
    log.debug(message)
  }
}
