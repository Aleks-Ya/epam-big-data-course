package lesson3

import lesson3.ioc.AppContext
import lesson3.kafka.KafkaService
import lesson3.spark.TrafficAnalyzer
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory

object Main {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {
    val ssc: StreamingContext = AppContext.streamingContext
    val kafkaService: KafkaService = AppContext.kafkaService
    try {
      val receiver = AppContext.receiver
      val stream = ssc.receiverStream(receiver)
      new TrafficAnalyzer(stream)
      kafkaService.start()
      ssc.start()
      ssc.awaitTermination()
    } finally {
      try {
        if (ssc != null) {
          ssc.stop()
        }
      } catch {
        case e: Exception => log.error(e.getMessage, e)
      }
      try {
        if (kafkaService != null) {
          kafkaService.stop()
        }
      } catch {
        case e: Exception => log.error(e.getMessage, e)
      }
    }
  }

}