package lesson3.net

import java.util.concurrent.{Callable, ExecutorService, Executors}

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.streaming.receiver.Receiver
import org.pcap4j.core.{BpfProgram, PcapHandle, Pcaps}
import org.pcap4j.packet.{IpV4Packet, Packet}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class NetworkInterfaceReceiver extends Receiver[TcpPacket](StorageLevels.MEMORY_ONLY) {
  private val log = LoggerFactory.getLogger(getClass)
  private val tcpFilter = "tcp"
  private var pool: ExecutorService = _

  override def onStart() {
    log.debug("Starting " + getClass.getSimpleName)
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
              try {
                while (!Thread.currentThread().isInterrupted) {
                  val packet: Packet = handle.getNextPacket
                  if (packet != null) {
                    val payload = packet.getPayload
                    if (payload.isInstanceOf[IpV4Packet]) {
                      store(convertToTcpPacket(payload))
                    }
                  }
                }
              } catch {
                case e: Exception => log.error("Exception, restart", e)
              }
            }
          }
        }
      })

    pool = Executors.newFixedThreadPool(callables.length)
    pool.invokeAll(callables.asJava)

    log.info(getClass.getSimpleName + " started")
  }

  private def convertToTcpPacket(packet: Packet) = {
    val ep = packet.asInstanceOf[IpV4Packet]
    val srcAddr = ep.getHeader.getSrcAddr.getHostAddress
    val length = ep.length
    val tcpPacket = new TcpPacket(srcAddr, length)
    log.debug("Created: " + tcpPacket)
    tcpPacket
  }

  override def onStop(): Unit = {
    if (pool != null) pool.shutdownNow()
  }
}
