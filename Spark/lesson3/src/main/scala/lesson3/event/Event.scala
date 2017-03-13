package lesson3.event

import lesson3.event.EventType.EventType

trait Event {
  def ip: String
  def eventType: EventType
}
