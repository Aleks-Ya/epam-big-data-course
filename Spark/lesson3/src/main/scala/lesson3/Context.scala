package lesson3

import lesson3.event.service.{ConsoleEventService, EventService}
import lesson3.hive.{HardcodedHiveService, HiveService}
import lesson3.kafka.{KafkaService, KafkaServiceImpl}

object Context {
  val kafkaService: KafkaService = KafkaServiceImpl
  val eventService: EventService = new ConsoleEventService
  val hiveService: HiveService = HardcodedHiveService
}
