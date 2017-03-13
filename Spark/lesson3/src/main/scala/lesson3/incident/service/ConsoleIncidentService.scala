package lesson3.incident.service

import lesson3.incident.Incident

class ConsoleIncidentService extends IncidentService {
  override def sendKafkaEvent(event: Incident): Unit = println("EVENT: " + event)
}
