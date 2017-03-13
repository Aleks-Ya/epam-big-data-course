package lesson3.properties

import java.io.InputStreamReader
import java.util.Properties

import org.slf4j.LoggerFactory

object ApplicationProperties {
  private val log = LoggerFactory.getLogger(ApplicationProperties.getClass)

  private val properties = new Properties
  private val propFile = "application.properties"
  private val is = ApplicationProperties.getClass.getClassLoader.getResourceAsStream(propFile)
  if (is == null) {
    throw new ExceptionInInitializerError("Resource not found: " + propFile)
  }
  properties.load(new InputStreamReader(is))

  private val kafkaServerNames = "kafka.bootstrap.servers"

  def kafkaBootstrapServers: String = {
    val p = properties.getProperty(kafkaServerNames)
    log.debug("Property {}={}", Seq(kafkaServerNames, p))
    p
  }
}
