package lesson1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import scala.io._
import java.io.File
import org.scalatest.BeforeAndAfterAll

class RealDataTest extends FlatSpec with BeforeAndAfterAll {

  var sc: SparkContext = null

  override def beforeAll() {
    val conf = new SparkConf().setAppName("RealDataTest").setMaster("local")
    sc = new SparkContext(conf)
  }

  "Analyse the access log file" should "print Top5 and save all entities to output file" in {

    val inputFile = new File(getClass.getClassLoader.getResource("access.log").getFile)
    println("Input file: " + inputFile)
    assert(inputFile.exists())

    val outputFile = new File("target/output.csv")
    println("Output file: " + outputFile)

    val al = new AccessLogTask()
    al.processFile(sc, inputFile, outputFile)
    val top5 = al.getTop5()
    println("Top5: " + top5)

    val size = outputFile.length()
    println("Output file size: " + size)
    assert(size == 4096L)
  }

  override def afterAll() {
    sc.stop()
  }
}