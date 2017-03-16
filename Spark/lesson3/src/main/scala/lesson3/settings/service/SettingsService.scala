package lesson3.settings.service

import lesson3.settings.IpSettings

trait SettingsService extends Serializable {
  def getIpSettings(ip: String): IpSettings


}

object DefaultSettingsIp {
  val defaultSettingsIp = "NULL"
}
