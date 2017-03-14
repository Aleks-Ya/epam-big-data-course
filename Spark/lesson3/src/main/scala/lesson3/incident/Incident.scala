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

  override def toString = s"Incident(uuid=$uuid, timestamp=$timestamp, ip=$ip, incidentType=$incidentType, factValue=$factValue, thresholdValue=$thresholdValue, period=$period)"
}
