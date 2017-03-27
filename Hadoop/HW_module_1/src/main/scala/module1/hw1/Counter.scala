package module1.hw1

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.concurrent.Callable
import java.util.concurrent.locks.Lock

import module1.hw1.Processor.IdCountMap
import org.slf4j.LoggerFactory

class Counter(private val is: InputStream,
              private val joinedMap: IdCountMap,
              private val joinedMapLock: Lock)
  extends Callable[Unit] {
  private val log = LoggerFactory.getLogger(getClass)
  var lineProcessed = 0L

  override def call(): Unit = {
    val threadName = Thread.currentThread().getName
    log.info(s"Counter $threadName started")
    val reader = new BufferedReader(new InputStreamReader(is))
    var line: String = null
    val idCountMap = scala.collection.mutable.Map[Long, Int]()
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
    reader.close()
    log.info(s"Counter $threadName finished calculating. Processed $lineProcessed lines. Map size ${idCountMap.size}.")
    log.info(s"Counter $threadName started to join map")
    try {
      joinedMapLock.lock()
      Helper.joinMap(idCountMap, joinedMap, joinedMapLock)
    } finally {
      joinedMapLock.unlock()
    }
    log.info(s"Counter $threadName joined map. Joined map size ${joinedMap.size}")
  }

}
