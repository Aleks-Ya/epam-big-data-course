package module1.hw1

import java.io.{File, FileWriter}
import java.util.concurrent.locks.Lock

import module1.hw1.Processor.IdCountMap

object Helper {

  def parseIPinYouID(line: String): String = {
    val idStart = line.indexOf("\t") + 1
    val idEnd = line.indexOf("\t", idStart)
    line.substring(idStart, idEnd).intern()
  }

  def joinMap(fromMap: IdCountMap, toMap: IdCountMap, lock: Lock): Unit = {
    try {
      lock.lock()
      fromMap.foreach(entry => {
        var count = entry._2
        if (toMap.contains(entry._1)) {
          count = toMap(entry._1) + entry._2
        }
        toMap += entry._1 -> count
      })
    } finally {
      lock.unlock()
    }
  }

  def writeToLocalFile(top: List[(String, Int)], file: File): Unit = {
    val writer = new FileWriter(file)
    var n = 0
    top.foreach(t => {
      n += 1
      writer.append(s"$n ${t._1} ${t._2}\n")
    })
    writer.close()
  }
}
