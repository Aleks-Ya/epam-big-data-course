package lesson3.ipinfo

object IpInfoCalculator extends Serializable {
  /**
    * Download rate (MB/sec for L last seconds).
    * Used for threshold.
    */
  def calculateDownloadRate(ipInfo: IpInfo, period: Long): Double = {
    val thresholdSum = ipInfo.history.take(period.toInt).sum
    thresholdSum / period
  }

  /**
    * Downloaded total (MB for N last seconds).
    * Used for limit.
    */
  def calculateDownloadedTotal(ipInfo: IpInfo, period: Long): Long = {
    val thresholdSum = ipInfo.history.take(period.toInt).sum
    thresholdSum / period
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
