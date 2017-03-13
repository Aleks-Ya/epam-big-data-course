package lesson3.incident

object IncidentType extends Enumeration {
  type EventType = Value
  val ThresholdExceed, ThresholdNorm, LimitExceed, LimitNorm = Value
}
