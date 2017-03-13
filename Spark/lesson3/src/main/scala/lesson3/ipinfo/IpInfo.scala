package lesson3.ipinfo

class IpInfo(var history: SizeBoundedList[Long],
             var historyThresholdSum: Long,
             var historyLimitSum: Long,
             var thresholdExceed: Boolean = false,
             var limitExceed: Boolean = false)
  extends Serializable {
  override def toString: String =
    "%s(history=%s)".format(classOf[IpInfo].getSimpleName, history)
}
