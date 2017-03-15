package lesson3.spark

import java.io.Serializable

import lesson3.incident.IncidentHelper
import lesson3.ioc.{AppContext, AppProperties}
import lesson3.ipinfo.{IpInfo, IpInfoHelper}
import lesson3.net.TcpPacket
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream

class TrafficAnalyzer(private val stream: DStream[TcpPacket])
  extends Serializable {
  val windowInterval = Minutes(AppProperties.statisticsIntervalMin.toInt)

  stream
    .map(packet => (packet.ip, packet))
    .reduceByKey((p1, p2) => new TcpPacket(p1.ip, p1.size + p2.size))
    .map(pair => {
      val ip = pair._1
      val packet = pair._2
      val settings = TrafficAnalyzerHelper.settingsByIp(ip)
      val tuple = (ip, (packet, settings))
      TrafficAnalyzerHelper.logInfo("Tuple created: " + tuple)
      tuple
    })
    .updateStateByKey((pairs, ipInfoOpt: Option[IpInfo]) => {
      TrafficAnalyzerHelper.logInfo("Process pairs: " + pairs)
      assert(pairs.size <= 1)
      var ipInfo: IpInfo = null
      if (pairs.nonEmpty) {
        val pair = pairs.head
        val packet = pair._1
        val settings = pair._2
        val ip = packet.ip
        ipInfo = ipInfoOpt.getOrElse(IpInfoHelper.newIpInfo(ip, settings))
        ipInfo.history.append(packet.size)
        TrafficAnalyzerHelper.processThreshold(ip, settings, ipInfo)
        TrafficAnalyzerHelper.processLimit(ip, settings, ipInfo)
      } else {
        ipInfo = ipInfoOpt.get
        ipInfo.history.append(0)
      }
      if (ipInfo.history.sum > 0) {
        Some(ipInfo)
      } else {
        TrafficAnalyzerHelper.logInfo("Remove state: " + ipInfo)
        None
      }
    })
    .window(windowInterval, windowInterval)
    .map(tuple => IncidentHelper.newIpStatisticsRow(tuple._1, tuple._2))
    .foreachRDD(rdd => AppContext.hiveService.saveHourStatistics(rdd))
}


