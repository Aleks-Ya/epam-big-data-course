package lesson3.event

import lesson3.event.EventType.EventType

trait Event {
  def ip: String

  def eventType: EventType

  override def toString: String =
    "%s(ip=%s,eventType=%s)".format(this.getClass.getSimpleName, ip, eventType)
}
