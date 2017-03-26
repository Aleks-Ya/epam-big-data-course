package module1.hw1

import java.io.InputStream
import java.util.concurrent.Executors

import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


object Processor {
  private val log = LoggerFactory.getLogger(getClass)

  def process(streams: List[InputStream], topElements: Int): Map[String, Int] = {
    val counters = streams.map(is => new Counter(is))
    log.info("Threads count: " + counters.size)
    val pool = Executors.newFixedThreadPool(counters.size)
    log.info("Invoke counters")
    val futures = pool.invokeAll(counters.asJava).asScala
    log.info("Counters invoked")
    val idCountMaps = futures.map(future => future.get).toList
    log.info("Counters finished" + idCountMaps.size)
    pool.shutdown()
    val joinedMap = Helper.joinMaps(idCountMaps)
    log.info("Join finished Map size=" + idCountMaps.size)
    val sortedMap = Helper.sortMap(joinedMap)
    log.info("Map sorted")
    val top100Map = sortedMap.take(topElements)
    log.info("top 100 found")
    top100Map
  }
}
