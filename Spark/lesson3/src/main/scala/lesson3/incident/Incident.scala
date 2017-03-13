package lesson3.incident

import lesson3.incident.IncidentType.EventType

class Incident(
                val uuid: String,
                val timestamp: String,
                val ip: String,
                val incidentType: EventType,
                val factValue: Double,
                val thresholdValue: Double,
                val period: Long) {

  override def toString: String =
    "%s(ip=%s,eventType=%s)".format(this.getClass.getSimpleName, ip, incidentType)
}
