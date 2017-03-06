package lesson2
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import lesson2.loader.CsvResourceLoader

class AirportReportTest extends FlatSpec with BeforeAndAfterAll {
  var sc: SparkContext = null
  var sql: SQLContext = null

  override def beforeAll {
    val conf = new SparkConf().setAppName("SmallDataTest").setMaster("local")
    sc = new SparkContext(conf)
    sql = new SQLContext(sc)
  }

  "Processor" should "return correct Result" in {
    val loader = new CsvResourceLoader(sql, "lesson2/airports.csv", "lesson2/carriers.csv", "lesson2/flights.csv")
    val processor = new Processor(loader)
    val result = processor.calculate

    result.biggestCarrier shouldEqual "Southwest Airlines Co."
    result.busiestAirportsUsaJuneAug should contain inOrderOnly ("Perry-Warsaw", "LaGuardia")
    result.flightsNycJune2007 shouldEqual 1
    result.flightsPerCarrierIn2007 shouldEqual 3
  }

  override def afterAll {
    sc.stop
  }
}