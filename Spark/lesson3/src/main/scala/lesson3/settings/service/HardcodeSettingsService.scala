package lesson3.settings.service

import lesson3.settings.{Category, IpSettings, Settings}


class HardcodeSettingsService extends SettingsService {
//  val nullThreshold = new Settings("null", Category.Threshold, 0, 0)
//  val nullLimit = new Settings("null", Category.Limit, 0, 0)
  private val settings = List(NullThresholdSettings, NullLimitSettings)

  override def getSettings: List[Settings] = {
    settings
  }

  override def getSettings(ip: String): IpSettings = {
    new IpSettings(NullThresholdSettings, NullLimitSettings)
  }

  override def getSettingsByIp(): Map[String, IpSettings] = {
//    val map = new HashMap[String, IpSettings]()
    getSettings.map(s => s.ip -> getSettings(s.ip)).toMap
  }
}

object NullThresholdSettings extends Settings("null", Category.Threshold, 1000, 2) {}
object NullLimitSettings extends Settings("null", Category.Limit, 2000, 1) {}