package lesson2
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.FlatSpec
import org.scalatest.Matchers.convertToStringShouldWrapper
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
class AirportReportTest extends FlatSpec with BeforeAndAfterAll {
  var sc: SparkContext = null
  var sql: SQLContext = null

  override def beforeAll() {
    val conf = new SparkConf().setAppName("SmallDataTest").setMaster("local")
    sc = new SparkContext(conf)
    sql = new SQLContext(sc)
  }

  "Process lines from the access log" should "return Top5" in {
    val airportsDf = loadCsvFromResource("airports.csv")
    airportsDf.registerTempTable("airports")
    val carriersDf = loadCsvFromResource("carriers.csv")
    carriersDf.registerTempTable("carriers")
    val flightsDf = loadCsvFromResource("flights.csv")
    flightsDf.registerTempTable("flights")

    //3.	Count total number of flights per carrier in 2007 
    val flightCount2007 = sql.table("flights").where("Year=2007").count()
    println("flightCount2007:" + flightCount2007)
    val carriersCount = sql.table("carriers").count()
    val flightsPerCarrier = flightCount2007 / carriersCount
    println("Flights per carrier in 2007:" + flightsPerCarrier)

    //4.	The total number of flights served in Jun 2007 by NYC 
    val nycJune = sql.sql(
      """SELECT COUNT(*) AS FlightsToNycInJune 
         FROM flights AS f JOIN airports AS a 
         ON f.Origin = a.iata OR f.Dest = a.iata 
         WHERE a.city='New York' AND f.Month=6""")
    nycJune.show

    //5.	Find five most busy airports in US during Jun 01 - Aug 31 (make #4). 
    val busyIata = sql.sql(
      """SELECT a.iata, COUNT(a.iata) AS FlightCount 
         FROM flights AS f JOIN airports AS a 
         ON f.Origin = a.iata OR f.Dest = a.iata 
         WHERE a.country = 'USA' AND f.Month IN(6,7,8)
         GROUP BY a.iata 
         ORDER BY COUNT(a.iata) DESC
         LIMIT 2""")
    busyIata.registerTempTable("busyIata")
    val busyAirports = sql.sql(
      """SELECT a.airport, FlightCount 
         FROM busyIata AS i JOIN airports AS a 
         ON i.iata = a.iata""")
    busyAirports.show

    //6.	Find the carrier who served the biggest number of flights 
    val biggestCarrierCode = sql.sql(
      """SELECT c.Code, COUNT(c.Code) AS FlightCount 
         FROM flights AS f JOIN carriers AS c 
         ON f.UniqueCarrier = c.Code 
         GROUP BY c.Code 
         ORDER BY COUNT(c.Code) DESC
         LIMIT 1""").head().getString(0)
    println("The biggest carrier's code: " + biggestCarrierCode)
    val biggestCarrier = sql
      .table("carriers")
      .where(s"Code = '$biggestCarrierCode'")
      .head().getString(1)
    println("The biggest carrier: " + biggestCarrier)
  }

  def loadCsvFromResource(resource: String) = {
    val resourceURI = getClass.getResource(resource)
    assert(resourceURI != null)
    val df = sql.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(resourceURI.getPath.toString)
    df.show
    df
  }

  override def afterAll() {
    sc.stop()
  }
}