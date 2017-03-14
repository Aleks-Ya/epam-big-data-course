package lesson3.kafka

import lesson3.incident.Incident

class ConsoleKafkaService extends KafkaService {
  override def sendEvent(event: Incident): Unit = {
    println("Send Kafka event: " + event)
  }

  override def start(): Unit = None

  override def stop(): Unit = None
}
