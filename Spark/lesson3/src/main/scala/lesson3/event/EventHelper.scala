package lesson3.event

import lesson3.ipinfo.IpInfo
import lesson3.settings.IpSettings

object EventHelper extends Serializable {

  def isThresholdExceed(ipInfo: IpInfo, ipSettings: IpSettings): Boolean = {
    val threshold = ipSettings.threshold
    val period = threshold.period
    if (period <= 0) {
      throw new IllegalArgumentException("Threshold is incorrect: " + ipSettings)
    }
    val thresholdSum = ipInfo.history.take(period.toInt).sum
    val actualValue = thresholdSum / period
    actualValue > threshold.value
  }

  def isLimitExceed(ipInfo: IpInfo, ipSettings: IpSettings): Boolean = {
    val limit = ipSettings.limit
    val period = limit.period
    if (period <= 0) {
      throw new IllegalArgumentException("Limit is incorrect: " + ipSettings)
    }
    if (period <= 0) {
      throw new IllegalArgumentException("Threshold is incorrect: " + ipSettings)
    }
    val limitSum = ipInfo.history.take(period.toInt).sum
    val actualValue = limitSum / period
    actualValue > limit.value
    //    val limitSum = ipInfo.history.take(period.toInt).sum
    //    val actualValue = limitSum / period
    //    if (actualValue > limit.value) {
    //      if (!ipInfo.limitExceed) {
    //        val event = new EventImpl(ip, EventType.LimitExceed)
    //        Context.kafkaService.sendEvent(event)
    //        ipInfo.limitExceed = true
    //      }
    //    } else {
    //      if (ipInfo.limitExceed) {
    //        val event = new EventImpl(ip, EventType.LimitNorm)
    //        Context.kafkaService.sendEvent(event)
    //        ipInfo.limitExceed = false
    //      }
    //    }
  }
}
