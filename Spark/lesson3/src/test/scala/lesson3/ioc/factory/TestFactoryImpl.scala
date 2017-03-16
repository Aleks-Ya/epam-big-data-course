package lesson3.ioc.factory

import lesson3.ioc.{AppContext, AppProperties}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

class TestFactoryImpl extends Factory {
  private val log = LoggerFactory.getLogger(getClass)

  override def sparkContext: SparkContext = {
    val conf = new SparkConf()
      .setAppName("Test")
      .setMaster("local[2]")
      .set("spark.streaming.clock", "org.apache.spark.util.ManualClock")
    new SparkContext(conf)
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
    null
  }
}
