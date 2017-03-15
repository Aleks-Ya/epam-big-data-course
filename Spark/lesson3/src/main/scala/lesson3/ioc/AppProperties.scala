package lesson3.ioc

import java.io.InputStreamReader
import java.nio.file.Files
import java.util.Properties

import org.slf4j.LoggerFactory

object AppProperties {
  private val log = LoggerFactory.getLogger(AppProperties.getClass)

  private val properties = new Properties
  private val propFile = "application.properties"
  private val is = AppProperties.getClass.getClassLoader.getResourceAsStream(propFile)
  if (is == null) {
    throw new ExceptionInInitializerError("Resource not found: " + propFile)
  }
  properties.load(new InputStreamReader(is))
  log.info(s"All properties:\n$properties\n")

  def sparkAppName: String = {
    prop("spark.app.name")
  }

  def sparkMaster: String = {
    prop("spark.master")
  }

  def kafkaBootstrapServers: String = {
    prop("kafka.bootstrap.servers")
  }

  def kafkaServiceImpl: String = {
    prop("kafka.service.impl")
  }

  def sparkReceiverImpl: String = {
    prop("spark.receiver.impl")
  }

  def incidentServiceImpl: String = {
    prop("incident.service.impl")
  }

  def hiveServiceImpl: String = {
    prop("hive.service.impl")
  }

  def settingsServiceImpl: String = {
    prop("settings.service.impl")
  }

  def statisticsIntervalMin: String = {
    prop("statistics.interval.min")
  }

  def checkpointDirectory: String = {
    val dir = Files.createTempDirectory("checkpoint_").toString
    log.info(s"Checkpoint directory: $dir")
    dir
  }

  private def prop(propertyName: String) = {
    val value = properties.getProperty(propertyName)
    log.info(s"Property: $propertyName=$value")
    value
  }
}
