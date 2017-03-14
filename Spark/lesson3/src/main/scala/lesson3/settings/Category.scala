package lesson3.settings

object Category extends Enumeration {
  type Category = Value
  val Threshold, Limit = Value

  def fromInt(c: Int): Category = {
    c match {
      case 1 => Threshold
      case 2 => Limit
      case _ => throw new IllegalArgumentException("Incorrect category: " + c)
    }
  }
}
