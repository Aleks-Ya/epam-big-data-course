package lesson3.incident

import lesson3.incident.IncidentType.EventType

class IncidentImpl(val ip: String,
                   val incidentType: EventType) extends Incident {
}
