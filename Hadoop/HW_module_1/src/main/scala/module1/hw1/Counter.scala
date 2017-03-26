package module1.hw1

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.concurrent.Callable

class Counter(private val is: InputStream) extends Callable[Map[String, Int]] {

  override def call(): Map[String, Int] = {
    val reader = new BufferedReader(new InputStreamReader(is))
    var line: String = null
    val idCountMap = scala.collection.mutable.Map[String, Int]()
    while ( {
      line = reader.readLine
      line != null
    }) {
      val id = Parser.parseIPinYouID(line)
      if (idCountMap.contains(id)) {
        val count = idCountMap(id) + 1
        idCountMap += id -> count
      } else {
        idCountMap += id -> 1
      }
    }
    idCountMap.toMap
  }

}
