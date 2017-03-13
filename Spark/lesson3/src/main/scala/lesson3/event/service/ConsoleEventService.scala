package lesson3.event.service

import lesson3.event.Event

class ConsoleEventService extends EventService {
  override def sendKafkaEvent(event: Event): Unit = println("EVENT: " + event)
}
