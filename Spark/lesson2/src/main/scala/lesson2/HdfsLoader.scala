package lesson2

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

class HdfsLoader(
    sql: SQLContext,
    airportsFile: String,
    carriersFile: String,
    flightsFile: String) extends Loader {

  def loadAirports(): DataFrame = {
    val df = load(airportsFile)
    df.registerTempTable("airports")
    df
  }

  def loadCarriers(): DataFrame = {
    val df = load(carriersFile)
    df.registerTempTable("carriers")
    df
  }

  def loadFlights(): DataFrame = {
    val df = load(flightsFile)
    df.registerTempTable("flights")
    df
  }

  def load(file: String) = {
    sql.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("escape", "\\")
      .load(file)
  }
}