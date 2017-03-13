package lesson3.event

import lesson3.event.EventType.EventType

@deprecated
class LimitExceedEvent extends AbstractEvent("") {
  override def eventType: EventType = EventType.LimitExceed
}
