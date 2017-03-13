package lesson3.event.service

import lesson3.event.Event

trait EventService {
  def sendKafkaEvent(event: Event)
}
