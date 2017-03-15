package lesson3.ipinfo

object IpInfoCalculator extends Serializable {
  /**
    * Download rate (MB/sec for L last seconds).
    * Used for threshold.
    */
  def calculateDownloadRate(ipInfo: IpInfo, period: Long): Double = {
    var actualPeriod = period
    if (actualPeriod == 0) return 0
    if (actualPeriod > ipInfo.history.size) {
      actualPeriod = ipInfo.history.size
    }
    calculateDownloadedTotal(ipInfo, actualPeriod) / actualPeriod
  }

  /**
    * Downloaded total (MB for N last seconds).
    * Used for limit.
    */
  def calculateDownloadedTotal(ipInfo: IpInfo, period: Long): Long = {
    if (period < 0) {
      throw new IllegalArgumentException(s"Period $period is < 0")
    }
    val history = ipInfo.history
    var periodFromTail = history.size - period.toInt
    periodFromTail = if (periodFromTail >= 0) periodFromTail else 0
    val tail = history.drop(periodFromTail)
    tail.sum
  }

  /**
    * Downloaded total for 1 last hour (MB).
    * Used for traffic consumed.
    */
  def calculateDownloadedHour(ipInfo: IpInfo): Long = {
    calculateDownloadedTotal(ipInfo, 60 * 60)
  }

  /**
    * Average download speed for 1 last hour (MB/sec).
    * Used for average speed.
    */
  def calculateDownloadRateHour(ipInfo: IpInfo): Double = {
    calculateDownloadRate(ipInfo, 60 * 60)
  }

}
