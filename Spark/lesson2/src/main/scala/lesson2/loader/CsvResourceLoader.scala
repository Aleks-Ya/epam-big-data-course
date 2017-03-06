package lesson2.loader

import org.apache.spark.sql.SQLContext

class CsvResourceLoader(
    sql: SQLContext,
    airportsResource: String,
    carriersResource: String,
    flightsResource: String) extends AbstractLoader(sql) {

  def airportsFile: String = getResource(airportsResource)

  def carriersFile: String = getResource(carriersResource)

  def flightsFile: String = getResource(flightsResource)

  def getResource(resource: String) = {
    val resourceURI = getClass.getClassLoader.getResource(resource)
    assert(resourceURI != null)
    resourceURI.getPath.toString
  }

}