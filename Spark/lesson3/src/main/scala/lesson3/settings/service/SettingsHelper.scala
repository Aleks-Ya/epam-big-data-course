package lesson3.settings.service

import lesson3.settings.Category.Category
import lesson3.settings.{Category, IpSettings, Settings}

package object SettingsHelper {
  def isSettingsPairValid(settingsList: List[Settings]): Boolean = {
    false
  }

  def findSettingsByCategory(settingsList: List[Settings], category: Category): Option[Settings] = {
    settingsList.find(settings => settings.category == category)
  }

  def toIpSettings(settings: Option[List[Settings]], defaultSettings: IpSettings): IpSettings = {
    if (settings.nonEmpty) {
      val thresholdSettings = findSettingsByCategory(settings.get, Category.Threshold)
        .getOrElse(defaultSettings.threshold)
      val limitSettings = findSettingsByCategory(settings.get, Category.Limit)
        .getOrElse(defaultSettings.limit)
      new IpSettings(thresholdSettings, limitSettings)
    } else {
      defaultSettings
    }
  }

  def verifyDefaultSettings(settingsListOpt: Option[List[Settings]]): IpSettings = {
    if (settingsListOpt.isEmpty) {
      throw new IllegalArgumentException
    }
    val settingsList = settingsListOpt.get
    if (settingsList.size != 2) {
      throw new IllegalArgumentException
    }
    settingsList.foreach(settings => {
      if (!DefaultSettingsIp.defaultSettingsIp.equalsIgnoreCase(settings.ip)) throw new IllegalArgumentException()
    })
    val thresholdSettings = findSettingsByCategory(settingsList, Category.Threshold).get
    val limitSettings = findSettingsByCategory(settingsList, Category.Limit).get
    new IpSettings(thresholdSettings, limitSettings)
  }

  //  def findNullSettings(settings: List[Settings]): Option[Settings] = {
  //    settingsList.find(settings => settings.category == Category.Limit)
  //  }


}
