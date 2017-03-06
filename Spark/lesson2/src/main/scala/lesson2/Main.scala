package lesson2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class Main {
  def main(args: Array[String]): Unit = {
    var sc: SparkContext = null
    try {
      val conf = new SparkConf().setAppName("YablokovSpark2").setMaster("local")
      sc = new SparkContext(conf)
      val sql = new SQLContext(sc)

      val loader = new HdfsLoader(
        sql,
        "/tmp/iablokov/spark/lesson2/airports.csv",
        "/tmp/iablokov/spark/lesson2/carriers.csv",
        "/tmp/iablokov/spark/lesson2/flights.csv")
      val processor = new Processor(loader)
      val result = processor.calculate

      println("Biggest carrier: " + result.biggestCarrier)
      println("Busies airport: " + result.busiestAirportsUsaJuneAug)
      println("Flights to NYC: " + result.flightsNycJune2007)
      println("Flights per carrier: " + result.flightsPerCarrierIn2007)
      
    } finally {
      if (sc != null) {
        sc.stop
      }
    }
  }
}