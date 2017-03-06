package lesson2.loader

import org.apache.spark.sql.SQLContext

class HdfsLoader(
    sql: SQLContext,
    airportsFile: String,
    carriersFile: String,
    flightsFile: String) extends AbstractLoader(sql) {

  def airportsFile(): String = airportsFile
  def carriersFile(): String = carriersFile
  def flightsFile(): String = flightsFile
}