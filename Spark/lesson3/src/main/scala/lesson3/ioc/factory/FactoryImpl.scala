package lesson3.ioc.factory

import lesson3.ioc.{AppContext, AppProperties}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

class FactoryImpl extends Factory {
  private val log = LoggerFactory.getLogger(getClass)

  override def sparkContext: SparkContext = {
    log.debug("Starting SparkContext")
    val conf = new SparkConf()
      .setAppName(AppProperties.sparkAppName)
      .setMaster(AppProperties.sparkMaster)
    val sc = new SparkContext(conf)
    log.info("SparkContext started")
    sc
  }

  override def streamingContext: StreamingContext = {
    log.debug("Starting StreamingContext")
    val batchDuration = Seconds(1)
    val ssc = new StreamingContext(AppContext.sparkContext, batchDuration)
    ssc.checkpoint(AppProperties.checkpointDirectory)
    log.info("StreamingContext started")
    ssc
  }

  override def hiveContext: HiveContext = {
    log.debug("Starting HiveContext")
    try {
      val hc = new HiveContext(AppContext.sparkContext)
      log.info("HiveContext started")
      hc
    } catch {
      case e: Exception => log.error("Hive context isn't initialized", e)
        null
    }
  }
}
