package module1.hw1

import java.io.{File, FileInputStream}

import org.slf4j.LoggerFactory

object MainLocal {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    log.info("Start")
    val files = new File("""c:\tmp\ipinyou\""").listFiles().map(file => new FileInputStream(file)).toList
    //    val files = List(new FileInputStream("""c:\tmp\ipinyou\bid.20130612.txt"""))
    val threads = files.size
    //    val threads = 1
    val top100 = Processor.process(files, 100, threads)

    val outFile = new File("bid_result.txt")
    Helper.writeToLocalFile(top100, outFile)

    log.info("Top 100:\n" + top100.mkString("\n"))
    log.info("Finish")
  }
}

