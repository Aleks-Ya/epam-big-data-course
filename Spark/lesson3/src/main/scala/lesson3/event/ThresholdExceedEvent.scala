package lesson3.event

import lesson3.event.EventType.EventType

@deprecated
class ThresholdExceedEvent extends Event {
  override def eventType: EventType = EventType.ThresholdExceed

  override def ip: String = ???
}
