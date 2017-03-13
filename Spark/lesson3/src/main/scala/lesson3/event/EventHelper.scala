package lesson3.event

import lesson3.settings.IpSettings
import lesson3.{Context, IpInfo}

object EventHelper extends Serializable {

  def checkThreshold(ip: String, ipInfo: IpInfo, ipSettings: IpSettings): Unit = {
    val threshold = ipSettings.threshold
    val period = threshold.period
    if (period > 0) {
      val thresholdSum = ipInfo.history.take(period.toInt).sum
      val actualValue = thresholdSum / period
      if (actualValue > threshold.value) {
        if (!ipInfo.thresholdExceed) {
          val event = new EventImpl(ip, EventType.ThresholdExceed)
          Context.kafkaService.sendEvent(event)
          ipInfo.limitExceed = true
        }
      } else {
        if (ipInfo.thresholdExceed) {
          val event = new EventImpl(ip, EventType.ThresholdNorm)
          Context.kafkaService.sendEvent(event)
          ipInfo.limitExceed = false
        }
      }
    }
  }

  def checkLimit(ip: String, ipInfo: IpInfo, ipSettings: IpSettings): Unit = {
    val limit = ipSettings.limit
    val period = limit.period
    if (period > 0) {
      val limitSum = ipInfo.history.take(period.toInt).sum
      val actualValue = limitSum / period
      if (actualValue > limit.value) {
        if (!ipInfo.limitExceed) {
          val event = new EventImpl(ip, EventType.LimitExceed)
          Context.kafkaService.sendEvent(event)
          ipInfo.limitExceed = true
        }
      } else {
        if (ipInfo.limitExceed) {
          val event = new EventImpl(ip, EventType.LimitNorm)
          Context.kafkaService.sendEvent(event)
          ipInfo.limitExceed = false
        }
      }
    }
  }
}
