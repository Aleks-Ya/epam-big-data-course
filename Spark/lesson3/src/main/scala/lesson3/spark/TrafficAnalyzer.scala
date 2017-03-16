package lesson3.spark

import java.io.Serializable

import lesson3.incident.IncidentHelper
import lesson3.ioc.{AppContext, AppProperties}
import lesson3.net.TcpPacket
import lesson3.spark.TrafficAnalyzerSteps._
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream

class TrafficAnalyzer(private val stream: DStream[TcpPacket]) extends Serializable {
  val windowInterval = Minutes(AppProperties.statisticsIntervalMin.toInt)

  stream
    .map(mapIpToPacket)
    .reduceByKey(reducePacketSize)
    .map(addSettings)
    .updateStateByKey(updateIpInfo)
    .window(windowInterval, windowInterval)
    .map(tuple => IncidentHelper.newIpStatisticsRow(tuple._1, tuple._2))
    .foreachRDD(rdd => AppContext.hiveService.saveHourStatistics(rdd))
}


