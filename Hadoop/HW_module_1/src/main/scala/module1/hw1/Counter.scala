package module1.hw1

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.concurrent.Callable

import org.slf4j.LoggerFactory

class Counter(private val is: InputStream) extends Callable[Map[String, Int]] {
  private val log = LoggerFactory.getLogger(getClass)
  var lineProcessed = 0L

  override def call(): Map[String, Int] = {
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
        if (count % 20 == 0) log.debug(s"$id->$count")
        idCountMap += id -> count
      } else {
        idCountMap += id -> 1
      }
      lineProcessed += 1
    }
    val immutableMap = idCountMap.toMap
    log.info(s"Counter finished. Processed $lineProcessed lines. Map size ${immutableMap.size}.")
    immutableMap
  }

}
