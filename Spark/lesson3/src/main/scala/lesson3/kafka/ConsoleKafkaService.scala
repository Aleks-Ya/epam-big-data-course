package lesson3.kafka

import lesson3.event.Event

object ConsoleKafkaService extends KafkaService {
  override def sendEvent(event: Event): Unit = {
    println("Send Kafka event: " + event)
  }

  override def start(): Unit = None

  override def stop(): Unit = None
}
