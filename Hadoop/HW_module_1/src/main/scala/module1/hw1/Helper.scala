package module1.hw1

import module1.hw1.Processor.IdCountMap

import scala.collection.immutable.ListMap

object Helper {
  def parseIPinYouID(line: String): String = {
    val idStart = line.indexOf("\t") + 1
    val idEnd = line.indexOf("\t", idStart)
    line.substring(idStart, idEnd)
  }

  def joinTwoMaps(m1: IdCountMap, m2: IdCountMap): IdCountMap = {
    m1 ++ m2.map { case (k, v) => k -> (v + m1.getOrElse(k, 0)) }
  }

  def joinMaps(maps: List[IdCountMap]): IdCountMap = {
    maps.reduce((m1, m2) => joinTwoMaps(m1, m2))
  }

  @deprecated
  def sortMap(map: IdCountMap): IdCountMap = {
    ListMap(map.toSeq.sortWith(_._2 > _._2): _*)
  }

  @deprecated
  def takeTopN(map: IdCountMap, n: Int): IdCountMap = {
    val tops = Array.ofDim[(String, Int)](n)
    map.foreach(entry => {
      var break = false
      for (i <- 0 until n) {
        if (!break) {
          if (tops(i) == null || tops(i)._2 < entry._2) {
            tops(i) = entry
            break = true
          }
        }
      }
    })
    tops.filter(entry => entry != null).toMap
  }

}
