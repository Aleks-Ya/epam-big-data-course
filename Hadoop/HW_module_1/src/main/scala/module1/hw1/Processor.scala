package module1.hw1

import java.io.InputStream
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock

import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


object Processor {
  type IdCountMap = collection.mutable.Map[Long, Int]
  type IdCount = (Long, Int)
  private val log = LoggerFactory.getLogger(getClass)

  def process(streams: List[InputStream], topElements: Int, threads: Int): List[IdCount] = {
    val joinedMap = collection.mutable.Map[Long, Int]()
    val lock = new ReentrantLock()
    val counters = streams.map(is => new Counter(is, joinedMap, lock))
    log.info("Thread count: " + threads)
    log.info("Counter count: " + counters.size)
    val pool = Executors.newFixedThreadPool(threads)
    val futures = pool.invokeAll(counters.asJava).asScala
    log.info("Counters invoked")
    futures.foreach(_.get)
    pool.shutdown()
    log.info("Counters finished")
    log.info("Joined Map size=" + joinedMap.size)
    val fixedSizeList = new SortedFixedSizeList(topElements)
    joinedMap.foreach(entry => {
      fixedSizeList.add(entry)
      joinedMap.remove(entry._1)
    })
    log.info(s"top $topElements found")
    fixedSizeList.toList
  }
}

