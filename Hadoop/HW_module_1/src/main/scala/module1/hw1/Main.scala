package module1.hw1

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger

object Main {
  private val log = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    log.info("Start")
    val config = new Configuration()
    val url = new URI("hdfs://sandbox.hortonworks.com:8020")
    val fs = FileSystem.get(url, config)
    log.info("Status: " + fs.getStatus(new Path("/")))
  }
}

