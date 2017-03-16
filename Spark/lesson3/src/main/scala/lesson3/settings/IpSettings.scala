package lesson3.settings

class IpSettings(
                  val threshold: Settings,
                  val limit: Settings)
  extends Serializable {

  override def toString: String =
    "%s(threshold=%s,limit=%s)".format(classOf[IpSettings].getSimpleName, threshold, limit)


  def canEqual(other: Any): Boolean = other.isInstanceOf[IpSettings]

  override def equals(other: Any): Boolean = other match {
    case that: IpSettings =>
      (that canEqual this) &&
        threshold == that.threshold &&
        limit == that.limit
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(threshold, limit)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
