package lesson3.ipinfo

class IpStatistics(val timestamp: String,
                   val ip: String,
                   val trafficConsumed: Long,
                   val averageSpeed: Double
                  ) {
  override def toString: String =
    "%s(timestamp=%s, ip=%s, trafficConsumed=%d, averageSpeed=%s)"
      .format(getClass.getSimpleName, timestamp, ip, trafficConsumed, averageSpeed)


  def canEqual(other: Any): Boolean = other.isInstanceOf[IpStatistics]

  override def equals(other: Any): Boolean = other match {
    case that: IpStatistics =>
      (that canEqual this) &&
        timestamp == that.timestamp &&
        ip == that.ip &&
        trafficConsumed == that.trafficConsumed &&
        averageSpeed == that.averageSpeed
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(ip)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
