package lesson3.hive

import lesson3.ipinfo.IpStatistics
import lesson3.settings.Settings

trait HiveService {
  def readSettings(): List[Settings]

  def updateTop3FastestIp(ips: List[String]): Unit

  def updateHourStatistics(ipStatistics: IpStatistics): Unit
}
