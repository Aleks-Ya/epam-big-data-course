package lesson2

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

class CsvResourceLoader(
    sql: SQLContext,
    airportsResource: String,
    carriersResource: String,
    flightsResource: String) extends Loader {

  def loadAirports(): DataFrame = {
    val df = load(airportsResource)
    df.registerTempTable("airports")
    df
  }

  def loadCarriers(): DataFrame = {
    val df = load(carriersResource)
    df.registerTempTable("carriers")
    df
  }

  def loadFlights(): DataFrame = {
    val df = load(flightsResource)
    df.registerTempTable("flights")
    df
  }

  def load(resource: String) = {
    val resourceURI = getClass.getClassLoader.getResource(resource)
    assert(resourceURI != null)
    val df = sql.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(resourceURI.getPath.toString)
    df.show
    df
  }
}