package lesson3

import java.nio.file.Files

import lesson3.event.service.{ConsoleEventService, EventService}
import lesson3.hive.{HardcodedHiveService, HiveService}
import lesson3.kafka.{KafkaService, KafkaServiceImpl}
import lesson3.net.{NetworkInterfaceReceiver, TcpPacket}
import lesson3.settings.service.{HardcodedSettingsService, SettingsService}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Context {
  private val log = LoggerFactory.getLogger(getClass)

  val sparkContext: SparkContext = {
    val conf = new SparkConf().setAppName("YablokovSpark3").setMaster("local[2]")
    new SparkContext(conf)
  }

  val streamingContext: StreamingContext = {
    val batchDuration = Seconds(5)
    val ssc = new StreamingContext(sparkContext, batchDuration)
    ssc.checkpoint(Files.createTempDirectory("checkpoint_").toString)
    ssc
  }

  val hiveContext: HiveContext = {
    try {
      new HiveContext(sparkContext)
    } catch {
      case e: Exception => log.error("Hive context isn't initialized", e)
        null
    }
  }

  val receiver: Receiver[TcpPacket] = new NetworkInterfaceReceiver

  val kafkaService: KafkaService = KafkaServiceImpl
  val eventService: EventService = new ConsoleEventService
  val hiveService: HiveService = HardcodedHiveService
  val settingsService: SettingsService = HardcodedSettingsService
}
