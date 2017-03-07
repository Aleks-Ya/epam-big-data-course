package lesson2

import lesson2.loader.CsvResourceLoader
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class AirportReportTest extends FlatSpec with BeforeAndAfterAll {
  var sc: SparkContext = _
  var sql: SQLContext = _

  override def beforeAll {
    val conf = new SparkConf()
      .setAppName("SmallDataTest")
      .setMaster("local")
    setEventLogDir(conf)
    sc = new SparkContext(conf)
    sql = new SQLContext(sc)
  }

  "Processor" should "return correct Result" in {
    val loader = new CsvResourceLoader(sql, "lesson2/airports.csv", "lesson2/carriers.csv", "lesson2/flights.csv")
    val processor = new Processor(loader)
    val result = processor.calculate

    result.biggestCarrier shouldEqual "Southwest Airlines Co."
    result.busiestAirportsUsaJuneAug should contain inOrderOnly("Perry-Warsaw (01G)", "Livingston Municipal (00R)", "LaGuardia (LGA)")
    result.flightsNycJune2007 shouldEqual 1
    result.flightsPerCarrierIn2007 shouldEqual 3
  }

  private def setEventLogDir(conf: SparkConf) = {
    val logDir = sys.env.get("SPARK_HISTORY_FS_LOG_DIRECTORY")
    if (logDir.isDefined) {
      conf
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir", logDir.get)
      println("Set event log dir: " + logDir.get)
    }
  }

  override def afterAll {
    sc.stop
  }
}