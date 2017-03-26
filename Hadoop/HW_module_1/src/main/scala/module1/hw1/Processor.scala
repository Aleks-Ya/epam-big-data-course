package module1.hw1

import java.io.InputStream
import java.util.concurrent.Executors

import scala.collection.JavaConverters._

object Processor {
  def process(streams: List[InputStream], topElements: Int): Map[String, Int] = {
    val counters = streams.map(is => new Counter(is))
    val pool = Executors.newFixedThreadPool(counters.size)
    val futures = pool.invokeAll(counters.asJava).asScala
    pool.shutdown()
    val idCountMaps = futures.map(future => future.get).toList
    val joinedMap = Helper.joinMaps(idCountMaps)
    val sortedMap = Helper.sortMap(joinedMap)
    val top100Map = sortedMap.take(topElements)
    top100Map
  }
}
