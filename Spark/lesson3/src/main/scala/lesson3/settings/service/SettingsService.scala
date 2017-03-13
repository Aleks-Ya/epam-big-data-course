package lesson3.settings.service

import lesson3.settings.IpSettings

trait SettingsService extends Serializable {
  def getIpSettings(ip: String): IpSettings


}

object NullSettingsIp {
  val nullSettingsIp = "NULL"
}
