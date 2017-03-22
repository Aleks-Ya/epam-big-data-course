package lesson4

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.io.Source

object FileHelper {
  private val log = LoggerFactory.getLogger(getClass)

  def readDescriptions: String = {
    log.info("Enter readDescriptions")
    val path = resourceToPath("PropertyDesciptionEN.txt")
    Source.fromFile(path).getLines.mkString("\n")
  }

  private def resourceToPath(resource: String) = {
    log.info("Enter resourceToPath")
    val url = getClass.getClassLoader.getResource(resource)
    if (url == null) {
      throw new RuntimeException("Resource not found: " + resource)
    }
    val path = url.getFile
    log.info("Path to resource: " + path)
    path
  }

  def readLabels(ss: SparkSession): RDD[Int] = {
    log.info("Enter readLabels")
    val labelsPath = resourceToPath("Target.csv")
    val labelsRdd = ss.sparkContext.textFile(labelsPath).map(_.toInt)
    labelsRdd
  }

  def readObjects(ss: SparkSession): RDD[Array[String]] = {
    log.info("Enter readObjects")
    val vectorsPath = resourceToPath("Objects.csv")
    val vectorsRdd = ss.sparkContext.textFile(vectorsPath)
      .map(line => line.replaceAll(",", "."))
      .map(line => line.split(";"))
    vectorsRdd
  }

}
