package lesson3.net

import java.util.concurrent.{Callable, ExecutorService, Executors}

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.streaming.receiver.Receiver
import org.pcap4j.core.{PcapHandle, Pcaps}
import org.pcap4j.packet.{IpV4Packet, Packet}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class NetworkInterfaceReceiver extends Receiver[TcpPacket](StorageLevels.MEMORY_ONLY) {
  private val log = LoggerFactory.getLogger(getClass)
  private var pool: ExecutorService = _

  override def onStart() {
    log.debug("Starting NetworkInterfaceReceiver")

    val nifs = Pcaps.findAllDevs.asScala

    val myIps = nifs
      .flatMap(nif => nif.getAddresses.asScala)
      .map(address => address.getAddress.getHostAddress)
      .reduce(_ + ", " + _)
    log.info("This PC's IPs: " + myIps)

    val callables = nifs
      .map(nif => {
        log.debug("Start creating handle for NIF: " + nif.getName)
        val handle = new PcapHandle.Builder(nif.getName).build()
        log.info("Handle created for NIF: " + nif.getName)
        handle
      })
      .map(handle => {
        log.debug("Start creating callable for handler: " + handle)
        val callable = new Callable[Unit] {
          override def call(): Unit = {
            log.info("PcapHandle called: " + this)
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
        log.info("Callable created for handler: " + handle)
        callable
      }).toList

    log.debug("Thread count: " + callables.length)
    pool = Executors.newFixedThreadPool(callables.length)
    log.debug("Thread pool created")
    pool.invokeAll(callables.asJava)
    log.info("NetworkInterfaceReceiver started")
  }

  private def convertToTcpPacket(packet: Packet) = {
    val ep = packet.asInstanceOf[IpV4Packet]
    val srcAddr = ep.getHeader.getSrcAddr.getHostAddress
    val length = ep.length
    val tcpPacket = new TcpPacket(srcAddr, length)
    log.trace("Created: " + tcpPacket)
    tcpPacket
  }

  override def onStop(): Unit = {
    if (pool != null) pool.shutdownNow()
  }
}
