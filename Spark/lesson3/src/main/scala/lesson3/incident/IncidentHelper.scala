package lesson3.incident

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

import lesson3.ipinfo.{IpInfo, IpInfoCalculator}
import lesson3.settings.IpSettings
import lesson3.spark.StatisticsInfo
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

object IncidentHelper extends Serializable {
  private val log = LoggerFactory.getLogger(getClass)

  def createThresholdExceedIncident(ip: String, ipInfo: IpInfo, ipSettings: IpSettings): Option[Incident] = {
    val threshold = ipSettings.threshold
    val period = threshold.period
    if (period <= 0) {
      throw new IllegalArgumentException("Threshold is incorrect: " + ipSettings)
    }
    val factValue = IpInfoCalculator.calculateDownloadRate(ipInfo, period)
    if (factValue > threshold.value) {
      if (!ipInfo.thresholdExceed) {
        val incident = newIncident(ip, IncidentType.ThresholdExceed, factValue, threshold.value, period)
        return Some(incident)
      }
    } else {
      if (ipInfo.thresholdExceed) {
        val incident = newIncident(ip, IncidentType.ThresholdNorm, factValue, threshold.value, period)
        return Some(incident)
      }
    }
    None
  }

  def createLimitExceedIncident(ip: String, ipInfo: IpInfo, ipSettings: IpSettings): Option[Incident] = {
    val limit = ipSettings.limit
    val period = limit.period
    if (period <= 0) {
      throw new IllegalArgumentException("limit is incorrect: " + ipSettings)
    }
    val factValue = IpInfoCalculator.calculateDownloadedTotal(ipInfo, period)
    if (factValue > limit.value) {
      if (!ipInfo.limitExceed) {
        val incident = newIncident(ip, IncidentType.LimitExceed, factValue, limit.value, period)
        return Some(incident)
      }
    } else {
      if (ipInfo.limitExceed) {
        val incident = newIncident(ip, IncidentType.LimitNorm, factValue, limit.value, period)
        return Some(incident)
      }
    }
    None
  }

  def newIncident(ip: String,
                  incidentType: IncidentType.Value,
                  factValue: Double,
                  threshold: Double,
                  period: Long): Incident = {
    val uuid = UUID.randomUUID().toString
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    new Incident(uuid, timestamp, ip, incidentType, factValue, threshold, period)
  }

  def newIpStatisticsRow(ip: String, ipInfo: IpInfo): Row = {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    val trafficConsumed = IpInfoCalculator.calculateDownloadedHour(ipInfo)
    val averageSpeed = IpInfoCalculator.calculateDownloadRateHour(ipInfo)
    val row = Row(timestamp, ip, trafficConsumed, averageSpeed)
    log.debug("Row created: " + row)
    row
  }

  def newStatisticsRow(info: StatisticsInfo): Row = {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    val trafficConsumed = info.totalSize / 1024 * 1024 //MB
    val averageSpeed: Double = if (info.count != 0) info.totalSize / info.count else 0
    val row = Row(timestamp, info.ip, trafficConsumed, averageSpeed)
    log.debug("Row created: " + row)
    row
  }
}
