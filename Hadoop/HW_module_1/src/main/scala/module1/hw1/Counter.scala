package module1.hw1

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.concurrent.Callable

import module1.hw1.Processor.IdCountMap
import org.slf4j.LoggerFactory

class Counter(private val is: InputStream) extends Callable[IdCountMap] {
  private val log = LoggerFactory.getLogger(getClass)
  private val threadName = Thread.currentThread().getName
  var lineProcessed = 0L

  override def call(): collection.mutable.Map[String, Int] = {
    log.info(s"Counter $threadName started")
    val reader = new BufferedReader(new InputStreamReader(is))
    var line: String = null
    val idCountMap = scala.collection.mutable.Map[String, Int]()
    while ( {
      line = reader.readLine
      line != null
    }) {
      val id = Helper.parseIPinYouID(line)
      if (idCountMap.contains(id)) {
        val count = idCountMap(id) + 1
        if (count % 20 == 0) log.debug(s"$threadName: $id->$count")
        idCountMap += id -> count
      } else {
        idCountMap += id -> 1
      }
      lineProcessed += 1
    }
    log.info(s"Counter $threadName finished. Processed $lineProcessed lines. Map size ${idCountMap.size}.")
    idCountMap
  }

}
