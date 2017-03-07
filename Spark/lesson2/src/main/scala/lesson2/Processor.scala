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
    sc.setJobDescription("calculateFlightsPerCarrierIn2007")
    val flightCount2007 = sql.table("flights").where("Year=2007").count()
    val carriersCount = sql.table("carriers").count()
    val flightsPerCarrierIn2007 = flightCount2007 / carriersCount
    println("\n3.	Count total number of flights per carrier in 2007 (make #2 screenshot): " + flightsPerCarrierIn2007 + "\n")
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

    println("\n 4. The total number of flights served in Jun 2007 by NYC (#3): " + flightsNycJune2007 + "\n")
    flightsNycJune2007
  }

  /**
   * 5.	Find five most busy airports in US during Jun 01 - Aug 31 (make #4).
   */
  def calculateBusiestAirports = {
    //both version give the same result 
    //    val busiestAirports = calculateBusiestAirportsV1
    val busiestAirports = calculateBusiestAirportsV2
    println("\n5.	Find five most busy airports in US during Jun 01 - Aug 31 (make #4): " + busiestAirports + "\n")
    busiestAirports
  }

  def calculateBusiestAirportsV1 = {
    sc.setJobDescription("busyIata")
    val busyIata = sql.sql(
      """SELECT a.iata, COUNT(a.iata) AS FlightCount 
                 FROM flights AS f JOIN airports AS a 
                 ON f.Origin = a.iata OR f.Dest = a.iata 
                 WHERE a.country = 'USA' AND f.Month IN(6,7,8)
                 GROUP BY a.iata 
                 ORDER BY COUNT(a.iata) DESC
                 LIMIT 5""")
    busyIata.registerTempTable("busyIata")

    sc.setJobDescription("busiestAirports")
    val busiestAirports = sql.sql(
      """SELECT a.iata, a.airport 
             FROM busyIata AS i JOIN airports AS a 
             ON i.iata = a.iata""")
      .map(r => "%s (%s)".format(r.getString(1), r.getString(0)))
      .collect().toList
    busiestAirports
  }

  def calculateBusiestAirportsV2 = {
    sc.setJobDescription("usaAirports")
    val usaAirports = sql.table("airports").select("iata", "airport", "country").where("country = 'USA'")
    usaAirports.cache()
    usaAirports.show()

    sc.setJobDescription("flights678")
    val flights678 = sql.table("flights").select("Month", "Origin", "Dest").where("Month IN(6,7,8)")
    flights678.cache()
    flights678.show()

    sc.setJobDescription("flightsByOrigin")
    val flightsByOrigin = flights678.groupBy("Origin").agg(count("Origin").as("count"))
    flightsByOrigin.cache()
    flightsByOrigin.show()

    sc.setJobDescription("flightsByDest")
    val flightsByDest = flights678.groupBy("Dest").agg(count("Dest").as("count"))
    flightsByDest.cache()
    flightsByDest.show()

    sc.setJobDescription("flightFromUsa")
    val flightsFromUsa = flightsByOrigin.join(usaAirports, flightsByOrigin.col("Origin").as("airport") === usaAirports.col("iata")).select("count", "iata", "airport")
    flightsFromUsa.cache()
    flightsFromUsa.show()

    sc.setJobDescription("flightToUsa")
    val flightsToUsa = flightsByDest.join(usaAirports, flightsByDest.col("Dest").as("airport") === usaAirports.col("iata")).select("count", "iata", "airport")
    flightsToUsa.cache()
    flightsToUsa.show()

    sc.setJobDescription("allUsaflights")
    val allUsaFlights = flightsFromUsa.unionAll(flightsToUsa)
    allUsaFlights.cache()
    allUsaFlights.show()

    sc.setJobDescription("usaFlightsByIata")
    val usaFlightsByIata = allUsaFlights.groupBy("iata").agg(sum("count").as("countSum")).orderBy(col("countSum").desc).limit(5)
    usaFlightsByIata.cache()
    usaFlightsByIata.show()

    sc.setJobDescription("usaFlightsByAirport")
    val usaFlightsByAirport = usaFlightsByIata.join(allUsaFlights, "iata").distinct().select("iata", "airport")
    usaFlightsByAirport.cache()
    usaFlightsByAirport.show()

    val busiestAirports = usaFlightsByAirport.collect.map(r => "%s (%s)".format(r.getString(1), r.getString(0))).toList
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
    val biggestCarrierCode = biggestCarrierCodeRow.getString(0)

    try {
      val biggestCarrierName = sql
        .table("carriers")
        .where(s"Code = '$biggestCarrierCode'")
        .head().getString(1)
      println("\n6.	Find the carrier who served the biggest number of flights (make #5): " + biggestCarrierName + "\n")
      biggestCarrierName
    } catch {
      case e: NoSuchElementException => throw new NoSuchElementException("Carrier not found by code: " + biggestCarrierCode)
    }
  }

}

class Result(
    val flightsPerCarrierIn2007: Long,
    val flightsNycJune2007: Long,
    val busiestAirportsUsaJuneAug: List[String],
    val biggestCarrier: String) {
}