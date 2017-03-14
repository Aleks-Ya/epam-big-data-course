package lesson3.ipinfo

class IpInfo(val ip: String,
             var history: LongSizeBoundedList,
             var thresholdExceed: Boolean = false,
             var limitExceed: Boolean = false)
  extends Serializable {
  override def toString: String =
    "%s(ip=%s, history=%s)".format(classOf[IpInfo].getSimpleName, ip, history)
}
