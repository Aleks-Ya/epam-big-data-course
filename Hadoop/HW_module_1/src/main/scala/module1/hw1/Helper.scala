package module1.hw1

import scala.collection.immutable.ListMap

object Helper {
  def parseIPinYouID(line: String): String = {
    val idStart = line.indexOf("\t") + 1
    val idEnd = line.indexOf("\t", idStart)
    line.substring(idStart, idEnd)
  }

  def joinTwoMaps(m1: Map[String, Int], m2: Map[String, Int]): Map[String, Int] = {
    m1 ++ m2.map { case (k, v) => k -> (v + m1.getOrElse(k, 0)) }
  }

  def joinMaps(maps: List[Map[String, Int]]): Map[String, Int] = {
    maps.reduce((m1, m2) => joinTwoMaps(m1, m2))
  }

  def sortMap(map: Map[String, Int]): Map[String, Int] ={
    ListMap(map.toSeq.sortWith(_._2 > _._2):_*)
  }

}
