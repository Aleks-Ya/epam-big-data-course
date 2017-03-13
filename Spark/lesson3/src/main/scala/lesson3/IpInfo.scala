package lesson3

import lesson3.event.Event

class IpInfo(var history: SizeBoundedList[Long],
             var historyThresholdSum: Long,
             var historyLimitSum: Long,
             var lastThresholdEvent: Event = null,
             var lastLimitEvent: Event = null)
  extends Serializable {
  override def toString: String =
    "%s(history=%s)".format(classOf[IpInfo].getSimpleName, history)
}
