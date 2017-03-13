package lesson3.kafka

import java.util.Properties

import lesson3.incident.Incident
import lesson3.properties.ApplicationProperties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory

object KafkaServiceImpl extends KafkaService {
  private var producer: KafkaProducer[String, String] = _
  private val topic = "alerts"
  private val log = LoggerFactory.getLogger(KafkaServiceImpl.getClass)

  override def sendEvent(event: Incident): Unit = {
    if (producer != null) {
      log.debug("Send event to Kafka: " + event)
      val record = new ProducerRecord[String, String](topic, event.toString)
      producer.send(record)
    } else {
      log.error("Producer isn't initialized")
    }
  }

  override def start(): Unit = {
    log.info("Starting KafkaProducer")
    val props = new Properties()
    props.put("bootstrap.servers", ApplicationProperties.kafkaBootstrapServers)
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    producer = new KafkaProducer[String, String](props)
    log.info("KafkaProducer is started")
  }

  override def stop(): Unit = {
    if (producer != null) producer.close()
  }
}
