package lesson3.event

object EventType extends Enumeration {
  type EventType = Value
  val ThresholdExceed, ThresholdNorm, LimitExceed, LimitNorm = Value
}
