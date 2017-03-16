package lesson3.settings.service

import lesson3.hive.{DefaultLimitSettings, DefaultThresholdSettings, HiveServiceFake}
import lesson3.ioc.AppContext
import lesson3.settings.IpSettings

class HardcodedSettingsService extends SettingsService {
  var defaultSettings: IpSettings = _
  private val ipToIpSettings: Map[String, IpSettings] = {
    val ipToSettings = AppContext.hiveService.readSettings().groupBy(settings => settings.ip)
    defaultSettings = SettingsHelper.verifyDefaultSettings(ipToSettings.get(DefaultSettingsIp.defaultSettingsIp))
    ipToSettings.map(pair => (pair._1, SettingsHelper.toIpSettings(Some(pair._2), defaultSettings)))
  }

  override def getIpSettings(ip: String): IpSettings = {
    ipToIpSettings.getOrElse(ip, defaultSettings)
  }
}

object DefaultIpSettings extends IpSettings(DefaultThresholdSettings, DefaultLimitSettings) {}
