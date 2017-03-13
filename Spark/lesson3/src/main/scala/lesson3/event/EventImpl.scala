package lesson3.event

import lesson3.event.EventType.EventType

class EventImpl(val ip: String,
                val eventType: EventType) extends Event {
}
