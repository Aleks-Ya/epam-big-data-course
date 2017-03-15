package lesson3.hive

import lesson3.settings.service.NullSettingsIp
import lesson3.settings.{Category, Settings}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

class HiveServiceFake extends HiveService {
  private val log = LoggerFactory.getLogger(getClass)
  private val settings = List(
    new Settings(NullSettingsIp.nullSettingsIp, Category.Threshold, 2, 5),
    new Settings(NullSettingsIp.nullSettingsIp, Category.Limit, 5, 10)
  )

  override def readSettings(): List[Settings] = {
    settings
  }

  override def updateTop3FastestIp(ips: List[String]): Unit = {}

  override def saveHourStatistics(rdd: RDD[Row]): Unit = {
    log.info("Save statistics: " + rdd.collect.mkString)
  }
}
