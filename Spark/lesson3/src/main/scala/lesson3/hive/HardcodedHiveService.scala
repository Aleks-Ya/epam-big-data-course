package lesson3.hive

import lesson3.settings.service.NullSettingsIp
import lesson3.settings.{Category, Settings}

object HardcodedHiveService extends HiveService {
  private val settings = List(NullThresholdSettings, NullLimitSettings)

  override def readSettings(): List[Settings] = {
    settings
  }

  override def updateTop3FastestIp(ips: List[String]): Unit = {}

  override def updateHourStatistics(): Unit = {}

  object NullThresholdSettings extends Settings(NullSettingsIp.nullSettingsIp, Category.Threshold, 2, 10) {}
  object NullLimitSettings extends Settings(NullSettingsIp.nullSettingsIp, Category.Limit, 5, 20) {}

}
