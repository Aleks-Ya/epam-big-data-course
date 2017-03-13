package lesson3.net

import lesson3.settings.IpSettings

class TcpPacket(val ip: String, val size: Int, val settings: IpSettings) extends Serializable {
  override def toString: String = "Packet(ip=%s, size=%d)".format(ip, size)
}
