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
    val futures = pool.invokeAll(counters.asJava).asScala
    log.info("Counters invoked")
    val idCountMaps = futures.map(future => future.get).toList
    pool.shutdown()
    log.info("Counters finished: " + idCountMaps.size)
    val joinedMap = Helper.joinMaps(idCountMaps)//TODO join finished maps immediately
    log.info("Join finished Map size=" + joinedMap.size)
    val fixedSizeList = new SortedFixedSizeList(topElements)
    joinedMap.foreach(entry => {
      fixedSizeList.add(entry)
      joinedMap.remove(entry._1)
    })
    log.info(s"top $topElements found")
    fixedSizeList.toList
  }
}

