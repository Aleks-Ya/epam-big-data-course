package lesson2

import java.util.NoSuchElementException

import lesson2.loader.Loader
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class Processor(loader: Loader) {

  var carriers, airports, flights: DataFrame = _
  var sc: SparkContext = _

  def calculate: Result = {
    carriers = loader.loadCarriers
    airports = loader.loadAirports
    flights = loader.loadFlights
    sc = airports.sqlContext.sparkContext

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
  private def calculateFlightsPerCarrierIn2007 = {
    sc.setJobDescription("calculateFlightsPerCarrierIn2007")
    val flightCount2007 = flights.where("Year=2007").count()
    val carriersCount = carriers.count()
    val flightsPerCarrierIn2007 = flightCount2007 / carriersCount
    println("\n3.	Count total number of flights per carrier in 2007 (make #2 screenshot): " + flightsPerCarrierIn2007 + "\n")
    flightsPerCarrierIn2007
  }

  /**
    * 4.	The total number of flights served in Jun 2007 by NYC
    */
  private def calculateFlightsNycJune2007 = {
    sc.setJobDescription("calculateFlightsNycJune2007")

    val nycAirports = airports.select("iata", "city").where("city='New York'")
    nycAirports.explain()
    nycAirports.cache()

    val juneFlights = flights.select("Month", "Origin", "Dest").where("Month=6")
    juneFlights.explain()
    juneFlights.cache()

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
  private def calculateBusiestAirports = {
    sc.setJobDescription("calculateBusiestAirports")

    val usaAirports = airports.select("iata", "airport", "country").where("country = 'USA'")
    usaAirports.cache()
    val flights678 = flights.select("Month", "Origin", "Dest").where("Month IN(6,7,8)")
    flights678.cache()
    val flightsByOrigin = flights678.groupBy("Origin").agg(count("Origin").as("count"))
    flightsByOrigin.cache()
    val flightsByDest = flights678.groupBy("Dest").agg(count("Dest").as("count"))
    flightsByDest.cache()
    val flightsFromUsa = flightsByOrigin.join(usaAirports, flightsByOrigin.col("Origin").as("airport") === usaAirports.col("iata")).select("count", "iata", "airport")
    flightsFromUsa.cache()
    val flightsToUsa = flightsByDest.join(usaAirports, flightsByDest.col("Dest").as("airport") === usaAirports.col("iata")).select("count", "iata", "airport")
    flightsToUsa.cache()
    val allUsaFlights = flightsFromUsa.unionAll(flightsToUsa)
    allUsaFlights.cache()
    val usaFlightsByIata = allUsaFlights.groupBy("iata").agg(sum("count").as("countSum")).orderBy(col("countSum").desc).limit(5)
    usaFlightsByIata.cache()
    val usaFlightsByAirport = usaFlightsByIata.join(allUsaFlights, Seq("iata"), "left_outer").select("iata", "airport")
    usaFlightsByAirport.cache()
    val busiestAirports = usaFlightsByAirport.collect.map(r => "%s (%s)".format(r.getString(1), r.getString(0))).toList

    println("\n5.	Find five most busy airports in US during Jun 01 - Aug 31 (make #4): " + busiestAirports + "\n")
    busiestAirports
  }

  /**
    * 6.	Find the carrier who served the biggest number of flights
    */
  private def calculateBiggestCarrier = {
    sc.setJobDescription("calculateBiggestCarrier")
    val biggestCarrierCodeRow =
      flights
        .select("UniqueCarrier")
        .groupBy("UniqueCarrier")
        .agg(count("UniqueCarrier").alias("FlightCount"))
        .orderBy(col("FlightCount").desc)
        .head()
    val biggestCarrierCode = biggestCarrierCodeRow.getString(0)

    try {
      val biggestCarrierName =
        carriers
          .where(s"Code = '$biggestCarrierCode'")
          .head().getString(1)
      println("\n6.	Find the carrier who served the biggest number of flights (make #5): " + biggestCarrierName + "\n")
      biggestCarrierName
    } catch {
      case e: NoSuchElementException => throw new NoSuchElementException("Carrier not found by code: " + biggestCarrierCode)
    }
  }

}

class Result(val flightsPerCarrierIn2007: Long,
             val flightsNycJune2007: Long,
             val busiestAirportsUsaJuneAug: List[String],
             val biggestCarrier: String) {
}