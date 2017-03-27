package module1.hw1

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger

object Main {
  private val log = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    log.info("Start")
    val url = new URI(args(0))
    val config = new Configuration()
    val fs = FileSystem.get(url, config)
    val files = args(1)
    val statuses = fs.globStatus(new Path(files))
    val streams = statuses
      .map(status => status.getPath)
      .map(path => fs.open(path))
      .toList

    val threads = streams.size
    val top100 = Processor.process(streams, 100, threads)
    log.info("Top 100:\n" + top100.mkString("\n"))

    log.info("Finish")
  }
}

