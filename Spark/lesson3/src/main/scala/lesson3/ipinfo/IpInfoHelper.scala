package lesson3.ipinfo

import lesson3.net.TcpPacket
import lesson3.settings.IpSettings

object IpInfoHelper extends Serializable {
  def newIpInfo(settings: IpSettings): IpInfo = {
    val historyLength = settings.threshold.period max settings.limit.period
    val history = new SizeBoundedList[Long](historyLength)
    new IpInfo(history, 0, 0)
  }

  def addTcpPackage(ipInfo: IpInfo, tcpPacket: TcpPacket): IpInfo = {
    val size = tcpPacket.size
    ipInfo.history.append(size)
    ipInfo
  }
}
