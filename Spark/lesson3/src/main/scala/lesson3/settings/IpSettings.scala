package lesson3.settings

class IpSettings(
                  val threshold: Settings,
                  val limit: Settings)
  extends Serializable {
  override def toString: String =
    "%s(threshold=%s,limit=%s)".format(classOf[IpSettings].getSimpleName, threshold, limit)
}
