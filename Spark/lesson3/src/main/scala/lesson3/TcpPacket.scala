package lesson3

class TcpPacket(val ip: String, size: Int) extends Serializable {
  override def toString: String = "Packet(ip=%s, size=%d)".format(ip, size)
}
