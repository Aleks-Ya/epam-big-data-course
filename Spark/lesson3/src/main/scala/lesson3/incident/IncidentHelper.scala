package lesson3.incident

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

import lesson3.ipinfo.{IpInfo, IpInfoCalculator}
import lesson3.settings.IpSettings

object IncidentHelper extends Serializable {

  def isThresholdExceed(ip: String, ipInfo: IpInfo, ipSettings: IpSettings): Option[Incident] = {
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

  def isLimitExceed(ip: String, ipInfo: IpInfo, ipSettings: IpSettings): Option[Incident] = {
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
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_INSTANT)
    new Incident(uuid, timestamp, ip, incidentType, factValue, threshold, period)
  }
}
