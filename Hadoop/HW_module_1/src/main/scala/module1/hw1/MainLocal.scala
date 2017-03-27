package module1.hw1

import java.io.{File, FileInputStream}

import org.slf4j.LoggerFactory

object MainLocal {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    log.info("Start")
    val files = new File("""c:\tmp\ipinyou\""").listFiles().map(file => new FileInputStream(file)).toList
    val threads = files.size
    val top100 = Processor.process(files, 100, threads)

    val outFile = new File("target/bid_result.txt")
    Helper.writeToLocalFile(top100, outFile)
    log.info("Output file: " + outFile.getAbsolutePath)
    log.info("Finish")
  }
}

