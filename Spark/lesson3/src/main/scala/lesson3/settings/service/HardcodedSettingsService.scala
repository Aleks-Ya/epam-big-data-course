package lesson3.settings.service

import lesson3.Context
import lesson3.hive.HardcodedHiveService.{NullLimitSettings, NullThresholdSettings}
import lesson3.settings.IpSettings

class HardcodedSettingsService extends SettingsService {
  private val hiveSettings = Context.hiveService
  private val settings = hiveSettings.readSettings()

  override def getSettings(ip: String): IpSettings = {
    new IpSettings(NullThresholdSettings, NullLimitSettings)
  }

  override def getSettingsByIp(): Map[String, IpSettings] = {
    settings.map(s => s.ip -> getSettings(s.ip)).toMap
  }
}
