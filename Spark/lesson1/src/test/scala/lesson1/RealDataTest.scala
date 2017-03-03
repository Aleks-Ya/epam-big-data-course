package lesson1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import scala.io._
import java.io.File
import org.scalatest.BeforeAndAfterAll
import java.nio.file.Files
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
    println("Top5:\n" + top5)

    val browsers = al.getBrowsers()
    val browsersExp = "IE: 0\nMozilla: 12280\nOthers: 1221\n"
    println("Browsers:\n" + browsers)
    browsers shouldEqual browsersExp

    val size = outputFile.length()
    val lines = Files.lines(new File(outputFile, "part-00000").toPath()).toArray
    assert(size == 4096L)
    assert("ip32,57503,16273379".equals(lines(0)))
    assert("ip1165,66003,12474621".equals(lines(1)))
    assert("ip16,95122,12080603".equals(lines(2)))
    assert("ip75,0,0".equals(lines.last))
  }

  override def afterAll() {
    sc.stop()
  }
}