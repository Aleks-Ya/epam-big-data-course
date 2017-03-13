package lesson3.net

class TcpPacket(val ip: String, val size: Int) extends Serializable {
  override def toString: String = "Packet(ip=%s, size=%d)".format(ip, size)
}
