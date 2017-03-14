package lesson3.settings.service

import lesson3.ioc.AppContext
import lesson3.settings.IpSettings

class HardcodedSettingsService extends SettingsService {
  private var nullSettings: IpSettings = _
  private val ipToIpSettings: Map[String, IpSettings] = {
    val ipToSettings = AppContext.hiveService.readSettings().groupBy(settings => settings.ip)
    nullSettings = SettingsHelper.verifyNullSettings(ipToSettings.get(NullSettingsIp.nullSettingsIp))
    ipToSettings.map(pair => (pair._1, SettingsHelper.toIpSettings(Some(pair._2), nullSettings)))
  }

  override def getIpSettings(ip: String): IpSettings = {
    ipToIpSettings.getOrElse(ip, nullSettings)
  }
}
