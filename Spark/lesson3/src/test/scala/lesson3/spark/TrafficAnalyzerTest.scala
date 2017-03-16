package lesson3.spark

import lesson3.ioc.AppContext
import lesson3.net.TcpPacket
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{ClockWrapper, Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable

class TrafficAnalyzerTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  var sc: SparkContext = AppContext.sparkContext
  var ssc: StreamingContext = AppContext.streamingContext

  ignore should "mapIpToPacket" in {
    val queue = mutable.Queue(sc.parallelize(Seq(
      new TcpPacket("100.200.300.400", 3000)
    )))
    val dstream = ssc.queueStream(queue)
    val analyzer = new TrafficAnalyzer(dstream)
    ssc.start()
    ClockWrapper.advance(ssc, Seconds(65))
    ssc.awaitTermination()
  }
}
