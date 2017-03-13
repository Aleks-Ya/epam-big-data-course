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
        val event = new EventImpl(ip, EventType.LimitExceed)
        Context.kafkaService.sendEvent(event)
      }
    }
  }

}
