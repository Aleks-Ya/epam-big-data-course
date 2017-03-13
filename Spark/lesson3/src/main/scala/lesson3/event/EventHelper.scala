package lesson3.event

import lesson3.IpInfo
import lesson3.event.service.ConsoleEventService
import lesson3.settings.IpSettings

object EventHelper extends Serializable {
  private val service = new ConsoleEventService

  def checkThreshold(ip: String, ipInfo: IpInfo, ipSettings: IpSettings): Unit = {
    val threshold = ipSettings.threshold
    val period = threshold.period
    if (period > 0) {
      val thresholdSum = ipInfo.history.take(period.toInt).sum
      val actualValue = thresholdSum / period
      if (actualValue > threshold.value) {
        val event = new EventImpl(ip, EventType.LimitExceed)
        service.sendKafkaEvent(event)
      }
    }
  }

}
