package lesson3.hive

import lesson3.settings.Settings

trait HiveService {
  def readSettings(): List[Settings]

  def updateTop3FastestIp(ips: List[String]): Unit

  def updateHourStatistics(): Unit
}
