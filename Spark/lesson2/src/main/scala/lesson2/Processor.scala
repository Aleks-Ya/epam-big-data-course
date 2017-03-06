package lesson2

import org.apache.spark.sql.DataFrame
import lesson2.loader.Loader
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import java.util.NoSuchElementException

class Processor(loader: Loader) {

  var carriers: DataFrame = null
  var airports: DataFrame = null
  var flight: DataFrame = null
  var sql: SQLContext = null
  var sc: SparkContext = null

  def calculate: Result = {
    carriers = loader.loadCarriers
    airports = loader.loadAirports
    flight = loader.loadFlights
    sql = airports.sqlContext
    sc = sql.sparkContext

    val flightsPerCarrierIn2007 = calculateFlightsPerCarrierIn2007
    val flightsNycJune2007 = calculateFlightsNycJune2007
    val busiestAirports = calculateBusiestAirports
    val biggestCarrier = calculateBiggestCarrier

    new Result(
      flightsPerCarrierIn2007,
      flightsNycJune2007,
      busiestAirports,
      biggestCarrier)
  }

  /**
   * 3.	Count total number of flights per carrier in 2007
   */
  def calculateFlightsPerCarrierIn2007 = {
    sc.setJobDescription("flightCount2007")
    val flightCount2007 = sql.table("flights").where("Year=2007").count()
    sc.setJobDescription("carriersCount")
    val carriersCount = sql.table("carriers").count()
    sc.setJobDescription("flightsPerCarrierIn2007")
    val flightsPerCarrierIn2007 = flightCount2007 / carriersCount
    println("Flights per carrier in 2007:" + flightsPerCarrierIn2007)
    flightsPerCarrierIn2007
  }

  /**
   * 4.	The total number of flights served in Jun 2007 by NYC
   */
  def calculateFlightsNycJune2007 = {
    sc.setJobDescription("nycAirports")
    val nycAirports = sql.table("airports").where("city='New York'")
    nycAirports.explain()
    nycAirports.cache()

    sc.setJobDescription("juneFlights")
    val juneFlights = sql.table("flights").where("Month=6")
    juneFlights.explain()
    juneFlights.cache()

    sc.setJobDescription("flightsNycJune2007")
    val flightsNycJune2007 = juneFlights
      .join(nycAirports,
        juneFlights.col("Origin") === nycAirports.col("iata")
          || juneFlights.col("Dest") === nycAirports.col("iata"))
      .count()

    println("flightsNycJune2007: " + flightsNycJune2007)
    flightsNycJune2007
  }

  /**
   * 5.	Find five most busy airports in US during Jun 01 - Aug 31 (make #4).
   */
  def calculateBusiestAirports = {
    sc.setJobDescription("busyIata")
    val busyIata = sql.sql(
      """SELECT a.iata, COUNT(a.iata) AS FlightCount 
             FROM flights AS f JOIN airports AS a 
             ON f.Origin = a.iata OR f.Dest = a.iata 
             WHERE a.country = 'USA' AND f.Month IN(6,7,8)
             GROUP BY a.iata 
             ORDER BY COUNT(a.iata) DESC
             LIMIT 2""")
    busyIata.registerTempTable("busyIata")

    sc.setJobDescription("busiestAirportsUsaJuneAug")
    val busiestAirports = sql.sql(
      """SELECT a.airport 
         FROM busyIata AS i JOIN airports AS a 
         ON i.iata = a.iata""")
      .map(r => r.getString(0))
      .collect()

    println(busiestAirports)
    busiestAirports
  }

  /**
   * 6.	Find the carrier who served the biggest number of flights
   */
  def calculateBiggestCarrier = {
    sc.setJobDescription("calculateBiggestCarrier")
    val biggestCarrierCodeRow = sql
      .table("flights")
      .groupBy("UniqueCarrier")
      .agg(count("UniqueCarrier").alias("FlightCount"))
      .orderBy(col("FlightCount").desc)
      .head()
    println("biggestCarrierCode: " + biggestCarrierCodeRow)
    val biggestCarrierCode = biggestCarrierCodeRow.getString(0)

    try {
      val biggestCarrierName = sql
        .table("carriers")
        .where(s"Code = '$biggestCarrierCode'")
        .head().getString(1)
      println("biggestCarrierName: " + biggestCarrierName)
      biggestCarrierName
    } catch {
      case e: NoSuchElementException => throw new NoSuchElementException("Carrier not found by code: " + biggestCarrierCode)
    }
  }

}

class Result(
    val flightsPerCarrierIn2007: Long,
    val flightsNycJune2007: Long,
    val busiestAirportsUsaJuneAug: Array[String],
    val biggestCarrier: String) {
}