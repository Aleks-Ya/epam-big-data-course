package lesson3.settings.service

import lesson3.settings.{IpSettings, Settings}

trait SettingsService extends Serializable {
  def getSettings: List[Settings]

  def getSettings(ip: String): IpSettings

  def getSettingsByIp(): Map[String, IpSettings]
}
