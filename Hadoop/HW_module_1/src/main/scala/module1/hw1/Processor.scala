package module1.hw1

import java.io.InputStream
import java.util.concurrent.Executors

import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


object Processor {
  type IdCountMap = collection.mutable.Map[String, Int]
  type IdCount = (String, Int)
  private val log = LoggerFactory.getLogger(getClass)

  def process(streams: List[InputStream], topElements: Int): List[IdCount] = {
    val counters = streams.map(is => new Counter(is))
    log.info("Threads count: " + counters.size)
    val pool = Executors.newFixedThreadPool(counters.size)
    log.info("Invoke counters")
    val futures = pool.invokeAll(counters.asJava).asScala
    log.info("Counters invoked")
    val idCountMaps = futures.map(future => future.get).toList
    log.info("Counters finished " + idCountMaps.size)
    pool.shutdown()
    val joinedMap = Helper.joinMaps(idCountMaps)
    log.info("Join finished Map size=" + idCountMaps.size)
    val fixedSizeList = new SortedFixedSizeList(topElements)
    joinedMap.foreach(entry => {
      fixedSizeList.add(entry)
      joinedMap.remove(entry._1)
    })
    log.info(s"top $topElements found")
    fixedSizeList.toList
  }
}

