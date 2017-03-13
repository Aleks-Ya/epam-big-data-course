package lesson3.settings.service

import lesson3.settings.IpSettings

trait SettingsService extends Serializable {
  def getSettings(ip: String): IpSettings

  def getSettingsByIp: Map[String, IpSettings]
}
