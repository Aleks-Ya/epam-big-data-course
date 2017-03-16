package lesson3.net

class TcpPacket(val ip: String, val size: Int) extends Serializable {
  override def toString: String = "Packet(ip=%s, size=%d)".format(ip, size)

  def canEqual(other: Any): Boolean = other.isInstanceOf[TcpPacket]

  override def equals(other: Any): Boolean = other match {
    case that: TcpPacket =>
      (that canEqual this) &&
        ip == that.ip &&
        size == that.size
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(ip)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
