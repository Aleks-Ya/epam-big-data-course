package lesson2

import org.apache.spark.sql.DataFrame

class Processor(loader: Loader) {
  def calculate: Result = {
    val airports = loader.loadAirports
    val carriers = loader.loadCarriers
    val flight = loader.loadFlights
    val sql = airports.sqlContext
    
    //3.	Count total number of flights per carrier in 2007 
    val flightCount2007 = sql.table("flights").where("Year=2007").count()
    println("flightCount2007:" + flightCount2007)
    val carriersCount = sql.table("carriers").count()
    val flightsPerCarrierIn2007 = flightCount2007 / carriersCount
    println("Flights per carrier in 2007:" + flightsPerCarrierIn2007)

    //4.	The total number of flights served in Jun 2007 by NYC 
    val flightsNycJune2007 = sql.sql(
      """SELECT COUNT(*) AS FlightsToNycInJune 
         FROM flights AS f JOIN airports AS a 
         ON f.Origin = a.iata OR f.Dest = a.iata 
         WHERE a.city='New York' AND f.Month=6""").head().getLong(0)
    println("flightsNycJune2007: " + flightsNycJune2007)

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
    val busiestAirportsUsaJuneAug = sql.sql(
      """SELECT a.airport 
         FROM busyIata AS i JOIN airports AS a 
         ON i.iata = a.iata""")
         .map(r => r.getString(0))
         .collect()
    println(busiestAirportsUsaJuneAug)

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
    
    new Result(
        flightsPerCarrierIn2007,
        flightsNycJune2007,
        busiestAirportsUsaJuneAug,
        biggestCarrier
        )
  }
}

class Result(
    val flightsPerCarrierIn2007: Long,
    val flightsNycJune2007: Long,
    val busiestAirportsUsaJuneAug: Array[String],
    val biggestCarrier: String) {
}