package lesson3.spark

import lesson3.incident.IncidentHelper
import lesson3.ioc.AppContext
import lesson3.ipinfo.{IpInfo, IpStatistics}
import lesson3.settings.IpSettings
import org.slf4j.LoggerFactory

object TrafficAnalyzerHelper extends Serializable {
  private val log = LoggerFactory.getLogger(getClass)

  def settingsByIp(ip: String): IpSettings = {
    AppContext.settingsService.getIpSettings(ip)
  }

  def processThreshold(ip: String, settings: IpSettings, ipInfo: IpInfo): Unit = {
    log.info(s"Process threshold: $ip, $settings, $ipInfo")
    val incidentOpt = IncidentHelper.createThresholdExceedIncident(ip, ipInfo, settings)
    if (incidentOpt.nonEmpty) {
      AppContext.kafkaService.sendEvent(incidentOpt.get)
      ipInfo.thresholdExceed = true
    }
  }

  def processLimit(ip: String, settings: IpSettings, ipInfo: IpInfo): Unit = {
    log.info(s"Process limit: $ip, $settings, $ipInfo")
    val incidentOpt = IncidentHelper.createLimitExceedIncident(ip, ipInfo, settings)
    if (incidentOpt.nonEmpty) {
      AppContext.kafkaService.sendEvent(incidentOpt.get)
      ipInfo.limitExceed = true
    }
  }

  def processHourStatistics(ip: String, ipInfo: IpInfo): Unit = {
    log.info(s"Process statistics: $ip, $ipInfo")
    val statistics = IncidentHelper.newIpStatistics(ip, ipInfo)
    AppContext.hiveService.updateHourStatistics(statistics)
  }

  def writeToHive(statistics: IpStatistics): Unit = {
    log.info(s"Write to Hive: $statistics")
    AppContext.hiveService.updateHourStatistics(statistics)
  }

  def logDebug(message: String): Unit = {
    log.debug(message)
  }

  def logInfo(message: String): Unit = {
    log.info(message)
  }
}
