package module1.hw1

import java.io.{File, FileInputStream}

import org.slf4j.LoggerFactory

object MainLocal {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    log.info("Start")
        val files = new File("""c:\tmp\ipinyou\""").listFiles().map(file => new FileInputStream(file)).toList
//    val files = List(new FileInputStream("""c:\tmp\ipinyou\bid.20130612.txt"""))
    val top100Map = Processor.process(files, 100)
    println(top100Map)
    log.info("Finish")
  }
}

