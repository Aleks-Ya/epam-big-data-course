package lesson3.hive

import lesson3.settings.Settings
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

trait HiveService {
  def readSettings(): List[Settings]

  def updateTop3FastestIp(ips: List[String]): Unit

  def saveHourStatistics(rdd: RDD[Row]): Unit
}
