package lesson3.net

import java.util.concurrent.{Callable, ExecutorService, Executors}

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.streaming.receiver.Receiver
import org.pcap4j.core.{BpfProgram, PcapHandle, Pcaps}
import org.pcap4j.packet.{EthernetPacket, Packet}

import scala.collection.JavaConverters._

class NetworkInterfaceReceiver extends Receiver[TcpPacket](StorageLevels.MEMORY_ONLY) {
  private val tcpFilter = "tcp"
  private var pool: ExecutorService = _

  override def onStart() {
    val callables = Pcaps.findAllDevs.asScala
      .map(nif => new PcapHandle.Builder(nif.getName).build())
      .map(handle => {
        handle.setFilter(tcpFilter, BpfProgram.BpfCompileMode.OPTIMIZE)
        handle
      })
      .map(handle => {
        new Callable[Unit] {
          override def call(): Unit = {
            while (!Thread.currentThread().isInterrupted) {
              val packet: Packet = handle.getNextPacket
              if (packet != null) {
                val payload = packet.getPayload
                store(convertToTcpPacket(packet))
                println(payload)
              }
            }
          }
        }
      })

    pool = Executors.newFixedThreadPool(callables.length)
    pool.invokeAll(callables.asJava)
  }

  private def convertToTcpPacket(packet: Packet) = {
    val ep = packet.asInstanceOf[EthernetPacket]
    val srcAddr = ep.getHeader.getSrcAddr.toString
    val length = ep.length
    new TcpPacket(srcAddr, length, null)
  }

  override def onStop(): Unit = {
    if (pool != null) pool.shutdownNow()
  }
}
