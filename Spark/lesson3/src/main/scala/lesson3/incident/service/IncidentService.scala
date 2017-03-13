package lesson3.incident.service

import lesson3.incident.Incident

trait IncidentService {
  def sendKafkaEvent(event: Incident)
}
