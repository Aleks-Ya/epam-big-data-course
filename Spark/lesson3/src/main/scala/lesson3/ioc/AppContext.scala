package lesson3.ioc

import lesson3.hive.HiveService
import lesson3.incident.service.IncidentService
import lesson3.kafka.KafkaService
import lesson3.net.TcpPacket
import lesson3.settings.service.SettingsService
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object AppContext {
  private val log = LoggerFactory.getLogger(getClass)

  lazy val sparkContext: SparkContext = {
    log.debug("Starting SparkContext")
    val conf = new SparkConf()
      .setAppName(AppProperties.sparkAppName)
      .setMaster(AppProperties.sparkMaster)
    val sc = new SparkContext(conf)
    log.info("SparkContext started")
    sc
  }

  lazy val streamingContext: StreamingContext = {
    log.debug("Starting StreamingContext")
    val batchDuration = Seconds(1)
    val ssc = new StreamingContext(sparkContext, batchDuration)
    ssc.checkpoint(AppProperties.checkpointDirectory)
    log.info("StreamingContext started")
    ssc
  }

  lazy val hiveContext: HiveContext = {
    log.debug("Starting HiveContext")
    try {
      val hc = new HiveContext(sparkContext)
      log.info("HiveContext started")
      hc
    } catch {
      case e: Exception => log.error("Hive context isn't initialized", e)
        null
    }
  }

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
