package lesson1

import org.apache.spark._
import org.apache.spark.rdd._
import scala._
import java.io.Serializable
import scala.collection.mutable.StringBuilder
import java.nio.file.Files
import java.io._
import java.io.Closeable
class AccessLogTask extends Serializable {
  private var top5: String = null
  private var browsers: String = null

  def processFile(sc: SparkContext, inputFile: File, outputFile: File) {
    val lines = sc.textFile(inputFile.toURI.toString)
    val ipAvgBytesMap = processLines(sc, lines)

    deleteIfExists(sc, outputFile)

    ipAvgBytesMap
      .map(row => {
        val (ip, totalBytes, avgBytes) = row
        formatLine(ip, totalBytes, avgBytes)
      })
      .saveAsTextFile(outputFile.getAbsolutePath)
  }

  def deleteIfExists(sc: SparkContext, outputFile: File) {
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val path = new org.apache.hadoop.fs.Path(outputFile.toURI)
    val exists = fs.exists(path)
    if (exists) {
      fs.delete(path)
    }
  }

  def processLines(sc: SparkContext, lines: RDD[String]) = {
    val ieAccum = sc.accumulator(0L, "IE counter")
    val mozillaAccum = sc.accumulator(0L, "Mozilla counter")
    val otherAccum = sc.accumulator(0L, "Other browser counter")

    val browserRegex = """ "(\w+)/.*"$""".r
    val bytesRegex = """" \d{3} (\d+) """".r
    val ipBytesMap = lines.map(line =>
      {
        val ip = line.take(line.indexOf(" "))

        val bytes = bytesRegex.findFirstMatchIn(line).map(_ group 1).getOrElse("0").toLong

        val browser = browserRegex.findFirstMatchIn(line).map(_ group 1).getOrElse("")
        browser match {
          case "msie" => ieAccum += 1
          case "Mozilla" => mozillaAccum += 1
          case _ => otherAccum += 1
        }

        (ip, bytes)
      })

    ipBytesMap.count() //wait accumulators
    browsers = "IE: %d\nMozilla: %d\nOthers: %d\n".format(ieAccum.value, mozillaAccum.value, otherAccum.value)

    val ipTotalBytesMap = ipBytesMap.reduceByKey(_ + _)
    println("ipTotalBytesMap " + ipTotalBytesMap.collect().toSeq)

    val ipCountMap = sc.parallelize(ipBytesMap.countByKey().toSeq)

    val ipAvgBytesMap = ipTotalBytesMap.join(ipCountMap)
      .map({ case (ip: String, data: Tuple2[Long, Long]) => (ip, data._1, data._1 / data._2) })
      .sortBy(f = { row => row._2 }, ascending = false)

    val takeFive = ipAvgBytesMap.take(5)
    println("takeFive " + takeFive.toSeq)

    top5 = ipAvgBytesMap
      .take(5)
      .map(row => {
        val (ip, totalBytes, avgBytes) = row
        formatLine(ip, totalBytes, avgBytes) + "\n"
      })
      .reduce(_ + _)

    ipAvgBytesMap
  }

  def formatLine(ip: String, totalBytes: Long, avgBytes: Long) =
    new StringBuilder()
      .append(ip).append(",")
      .append(avgBytes).append(",")
      .append(totalBytes)
      .toString()

  def getTop5() = top5

  def getBrowsers() = browsers
}