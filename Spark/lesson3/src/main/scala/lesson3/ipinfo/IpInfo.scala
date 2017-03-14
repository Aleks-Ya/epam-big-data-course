package lesson3.ipinfo

/**
  * State stored in DStream#updateStateByKey.
  */
class IpInfo(val ip: String,
             var history: LongSizeBoundedList,
             var thresholdExceed: Boolean = false,
             var limitExceed: Boolean = false)
  extends Serializable {

  override def toString = s"IpInfo(ip=$ip, history=$history, thresholdExceed=$thresholdExceed, limitExceed=$limitExceed)"
}
