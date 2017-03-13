package lesson3.kafka

import lesson3.incident.Incident

trait KafkaService {
  def start(): Unit
  def stop():Unit
  def sendEvent(event: Incident): Unit
}
