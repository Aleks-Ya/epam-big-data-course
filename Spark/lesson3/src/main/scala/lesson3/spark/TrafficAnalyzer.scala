package lesson3.spark

import java.io.Serializable

import lesson3.incident.IncidentHelper
import lesson3.ioc.{AppContext, AppProperties}
import lesson3.ipinfo.IpInfo
import lesson3.net.TcpPacket
import lesson3.spark.TrafficAnalyzerSteps._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream

class TrafficAnalyzer(private val stream: DStream[TcpPacket]) extends Serializable {
  val windowInterval = Minutes(AppProperties.statisticsIntervalMin.toInt)

  stream
    .map(mapIpToPacket)
    .reduceByKey(reducePacketSize)
    .map(addSettings)
    .updateStateByKey(updateIpInfo)
    .foreachRDD((rdd: RDD[(String, IpInfo)]) => None)

  stream
    .window(windowInterval, windowInterval)
    .map((packet: TcpPacket) => (packet.ip, new StatisticsInfo(packet.ip, packet.size, 1)))
    .reduceByKey((i1: StatisticsInfo, i2: StatisticsInfo) => {
      val ip = i1.ip
      new StatisticsInfo(ip, i1.totalSize + i2.totalSize, i1.count + i2.count)
    })
    .map(tuple => IncidentHelper.newStatisticsRow(tuple._2))
    .foreachRDD(rdd => AppContext.hiveService.saveHourStatistics(rdd))
}

class StatisticsInfo(val ip: String, val totalSize: Long, val count: Long) extends Serializable {}

