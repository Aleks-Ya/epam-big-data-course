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
    log.info("FileSystem status: " + fs.getStatus)
    val files = args(0)
    val statuses = fs.globStatus(new Path(files))
    val counters = statuses
      .map(status => status.getPath)
      .map(path => fs.open(path))
      .toList

    val threads = counters.size
    val top100 = Processor.process(counters, 100, threads)
    log.info("Top 100:\n" + top100.mkString("\n"))

    log.info("Finish")
  }
}

