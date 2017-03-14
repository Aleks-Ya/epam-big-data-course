package lesson3.ioc

import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory

object LoggerHelper {
  private val log = LoggerFactory.getLogger(getClass.getSimpleName)

  def setLogLevel(logger: String, level: Level): Unit = {
    AppContext.sparkContext.parallelize(Seq("")).foreachPartition(x => {
      Logger.getLogger(logger).setLevel(level)
      log.warn(s"Log level for $logger changed to $level")
    })
  }
}
