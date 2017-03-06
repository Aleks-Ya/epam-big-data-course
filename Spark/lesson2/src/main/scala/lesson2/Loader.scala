package lesson2

import org.apache.spark.sql.DataFrame

trait Loader {
  def loadAirports(): DataFrame
  def loadCarriers(): DataFrame
  def loadFlights(): DataFrame
}