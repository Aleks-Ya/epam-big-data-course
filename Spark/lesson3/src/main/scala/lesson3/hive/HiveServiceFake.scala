package lesson3.hive

import lesson3.settings.service.DefaultSettingsIp
import lesson3.settings.{Category, Settings}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

class HiveServiceFake extends HiveService {
  private val log = LoggerFactory.getLogger(getClass)
  private val settings = List(DefaultThresholdSettings, DefaultLimitSettings)


  override def readSettings(): List[Settings] = {
    settings
  }

  override def updateTop3FastestIp(ips: List[String]): Unit = {}

  override def saveHourStatistics(rdd: RDD[Row]): Unit = {
    log.info("Save statistics: " + rdd.collect.mkString)
  }
}

object DefaultThresholdSettings
  extends Settings(DefaultSettingsIp.defaultSettingsIp, Category.Threshold, 2, 5) {
}

object DefaultLimitSettings
  extends Settings(DefaultSettingsIp.defaultSettingsIp, Category.Limit, 5, 10) {
}
