package module1.hw1

object Parser {
  def parseIPinYouID(line: String): String = {
    val idStart = line.indexOf("\t") + 1
    val idEnd = line.indexOf("\t", idStart)
    line.substring(idStart, idEnd)
  }
}
