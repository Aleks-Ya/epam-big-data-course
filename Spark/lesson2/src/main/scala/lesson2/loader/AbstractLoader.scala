package lesson2.loader

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

abstract class AbstractLoader(
    sql: SQLContext) extends Loader {

  def loadAirports = {
    loadAndRegister(airportsFile, "airports")
  }

  def loadCarriers = {
    loadAndRegister(carriersFile, "carriers")
  }

  def loadFlights = {
    loadAndRegister(flightsFile, "flights")
  }

  def loadAndRegister(file: String, tableName: String) = {
    val df = sql.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("escape", "\\")
      .load(file)
    df.registerTempTable(tableName)
    df.cache()
  }

  def airportsFile: String
  def carriersFile: String
  def flightsFile: String
}