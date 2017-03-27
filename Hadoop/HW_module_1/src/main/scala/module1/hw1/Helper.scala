package module1.hw1

import module1.hw1.Processor.IdCountMap

object Helper {
  def parseIPinYouID(line: String): String = {
    val idStart = line.indexOf("\t") + 1
    val idEnd = line.indexOf("\t", idStart)
    line.substring(idStart, idEnd).intern()
  }

  def joinTwoMaps(m1: IdCountMap, m2: IdCountMap): IdCountMap = {
    m1 ++ m2.map { case (k, v) => k -> (v + m1.getOrElse(k, 0)) }
  }

  def joinMaps(maps: List[IdCountMap]): IdCountMap = {
    maps.reduce((m1, m2) => joinTwoMaps(m1, m2))
  }
}
