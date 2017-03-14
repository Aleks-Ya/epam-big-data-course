package lesson3.net

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.streaming.receiver.Receiver

class FakeReceiver(private val intervalMillis: Long = 2000L)
  extends Receiver[TcpPacket](StorageLevels.MEMORY_ONLY) {

  private var thread: Thread = _
  private val packet1 = new TcpPacket("100.200.300.400", 1000)
  private val packet2 = new TcpPacket("200.300.400.500", 5000)

  override def onStart() {
    thread = new Thread() {
      override def run(): Unit = {
        while (!isInterrupted) {
          storePacket(packet1)
          storePacket(packet2)
          Thread.sleep(intervalMillis)
        }
      }
    }
    thread.setDaemon(true)
    thread.start()
  }

  private def storePacket(packet: TcpPacket): Unit = {
    println("Store packet: " + packet)
    store(packet)
  }

  override def onStop(): Unit = {
    if (thread != null) thread.interrupt()
  }
}
