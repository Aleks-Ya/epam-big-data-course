package lesson3.ipinfo

class IpStatistics(val timestamp: String,
                   val ip: String,
                   val trafficConsumed: Long,
                   val averageSpeed: Double
                  ) {
  override def toString: String =
    "%s(timestamp=%s, ip=%s, trafficConsumed=%d, averageSpeed=%s)"
      .format(getClass.getSimpleName, timestamp, ip, trafficConsumed, averageSpeed)
}
