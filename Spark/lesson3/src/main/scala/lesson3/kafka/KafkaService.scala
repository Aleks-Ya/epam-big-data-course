package lesson3.kafka

import lesson3.event.Event

trait KafkaService {
  def start(): Unit
  def stop():Unit
  def sendEvent(event: Event): Unit
}
