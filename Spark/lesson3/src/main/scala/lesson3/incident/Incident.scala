package lesson3.incident

import lesson3.incident.IncidentType.EventType

trait Incident {
  def ip: String

  def incidentType: EventType

  override def toString: String =
    "%s(ip=%s,eventType=%s)".format(this.getClass.getSimpleName, ip, incidentType)
}
