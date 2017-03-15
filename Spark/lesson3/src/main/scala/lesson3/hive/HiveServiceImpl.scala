package lesson3.hive

import lesson3.ioc.AppContext
import lesson3.ipinfo.IpStatistics
import lesson3.settings.{Category, Settings}
import org.slf4j.LoggerFactory

class HiveServiceImpl extends HiveService {
  private val log = LoggerFactory.getLogger(getClass)
  private val settings: List[Settings] =
    AppContext.hiveContext
      .table("settings")
      .map(row => new Settings(row.getString(0), Category.fromInt(row.getInt(1)), row.getDouble(2), row.getLong(3)))
      .collect.toList

  override def readSettings(): List[Settings] = {
    settings
  }

  override def updateTop3FastestIp(ips: List[String]): Unit = {
  }

  override def updateHourStatistics(statistics: IpStatistics): Unit = {
    log.info("Write statistics: " + statistics)
    AppContext.hiveContext.sql("INSERT OVERWRITE INTO statistics_by_hour(timestamp, ip, traffic_consumed, average_speed)" +
      s"VALUES (${statistics.timestamp},${statistics.ip},${statistics.trafficConsumed},${statistics.averageSpeed})")
  }
}
