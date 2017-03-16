package lesson3.ioc

import lesson3.hive.HiveService
import lesson3.incident.service.IncidentService
import lesson3.ioc.factory.Factory
import lesson3.kafka.KafkaService
import lesson3.net.TcpPacket
import lesson3.settings.service.SettingsService
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver
import org.slf4j.LoggerFactory

object AppContext {
  private val log = LoggerFactory.getLogger(getClass)

  lazy val factory: Factory = instance(AppProperties.factoryImpl)
  lazy val sparkContext: SparkContext = factory.sparkContext
  lazy val streamingContext: StreamingContext = factory.streamingContext
  lazy val hiveContext: HiveContext = factory.hiveContext
  lazy val receiver: Receiver[TcpPacket] = instance(AppProperties.sparkReceiverImpl)
  lazy val kafkaService: KafkaService = instance(AppProperties.kafkaServiceImpl)
  lazy val incidentService: IncidentService = instance(AppProperties.incidentServiceImpl)
  lazy val hiveService: HiveService = instance(AppProperties.hiveServiceImpl)
  lazy val settingsService: SettingsService = instance(AppProperties.settingsServiceImpl)

  private def instance[T](className: String): T = {
    val inst = Class.forName(className).newInstance().asInstanceOf[T]
    log.info("Class instantiated: " + inst)
    inst
  }
}
