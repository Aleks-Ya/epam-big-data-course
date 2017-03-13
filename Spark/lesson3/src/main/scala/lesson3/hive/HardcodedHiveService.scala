package lesson3.hive

import lesson3.settings.{Category, Settings}

object HardcodedHiveService extends HiveService {
  private val settings = List(NullThresholdSettings, NullLimitSettings)

  override def readSettings(): List[Settings] = {
    settings
  }

  override def updateTop3FastestIp(ips: List[String]): Unit = {}

  override def updateHourStatistics(): Unit = {}

  object NullThresholdSettings extends Settings("null", Category.Threshold, 1000, 2) {}

  object NullLimitSettings extends Settings("null", Category.Limit, 2000, 1) {}

}
