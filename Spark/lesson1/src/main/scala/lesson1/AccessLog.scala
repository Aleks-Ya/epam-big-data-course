package lesson1

import org.apache.spark._
import org.apache.spark.rdd._
import scala._
import java.io.Serializable
import scala.collection.mutable.StringBuilder

class AccessLog extends Serializable {
  private var top5: String = null
  private var csvFile: String = null

  def processFile(sc: SparkContext, file: String) {
    val lines = sc.textFile("hdfs://sandbox.hortonworks.com:8020/user/admin/spark/lesson1/acces.log")
    processLines(sc, lines)
  }

  def processLines(sc: SparkContext, lines: RDD[String]) {
    val bytesRegex = """" \d{3} (\d+) """".r
    val ipBytesMap = lines.map(line =>
      {
        val ip = line.take(line.indexOf(" "))
        val bytesOptional = bytesRegex.findFirstMatchIn(line).map(_ group 1)
        if (bytesOptional.isEmpty) throw new RuntimeException("Can't find bytes in " + line)
        val bytes = bytesOptional.get.toLong
        (ip, bytes)
      })

    println("ipLineMap " + ipBytesMap.collect().toSeq)

    val ipTotalBytesMap = ipBytesMap.reduceByKey(_ + _)
    println("ipTotalBytesMap " + ipTotalBytesMap.collect().toSeq)

    val ipCountMap = sc.parallelize(ipBytesMap.countByKey().toSeq)
    println("ipCountMap " + ipCountMap.collect().toSeq)

    val ipAverageMap = ipTotalBytesMap.join(ipCountMap)
      .map({ case (ip: String, data: Tuple2[Long, Long]) => (ip, data._1, data._1 / data._2) })
      .sortBy(f = { row => row._2 }, ascending = false)
    println("ipAverageMap sorted: " + ipAverageMap.collect().toSeq)

    val takeFive = ipAverageMap.take(5)
    println("takeFive " + takeFive.toSeq)
    
    
    val text = ipAverageMap
      .take(5)
      .map(row => {
        val (ip, totalBytes, avgBytes) = row;
        new StringBuilder()
          .append(ip).append(",")
          .append(avgBytes).append(",")
          .append(totalBytes)
          .append("\n").toString();
      }).reduce(_ + _)
    top5 = text
  }

  def getCsvFile() = csvFile

  def getTop5() = top5
}