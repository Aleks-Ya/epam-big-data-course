package lesson4

import lesson4.Category.Category

object DescriptionParser {
  var content: String = _
  private val patternTitle = """^(\d+)\) (\w[\w\s]*): (\w+)$""".r
  private val patternCategoryValue = """^\t(\d+): (\w[-\w\s/]*)$""".r

  lazy val allFields: Map[Int, Description] = parseFromString(content)
  lazy val numericFields: Map[Int, Description] = allFields.filter(tuple => tuple._2.category == Category.Numeric)
  lazy val categoricalFields: Map[Int, Description] = allFields.filter(tuple => tuple._2.category == Category.Categorical)

  def parseFromString(content: String): Map[Int, Description] = {
    val properties = content.split("\n\n").filter(_.nonEmpty)
    properties.map { property =>
      val lines = property.split("\n").filter(_.nonEmpty)
      val titleLine = lines.head
      val titleMathches = patternTitle.findFirstMatchIn(titleLine).get
      val id = titleMathches.group(1).toInt
      val title = titleMathches.group(2)
      val category = Category.fromString(titleMathches.group(3))

      val values = lines.tail.map(line => {
        val matches = patternCategoryValue.findFirstMatchIn(line)
          .getOrElse(throw new RuntimeException(s"Can't parse '$line'"))
        val id = matches.group(1).toInt
        val title = matches.group(2).trim
        (id, title)
      }).toList

      (id, new Description(id, title, category, values))
    }.toMap
  }
}

class Description(val id: Int, val title: String, val category: Category, val values: List[(Int, String)]) {}

object Category extends Enumeration {
  type Category = Value
  val Numeric, Categorical = Value

  def fromString(s: String): Category = {
    s.toUpperCase match {
      case "NUMERIC" => Numeric
      case "CATEGORICAL" => Categorical
      case _ => throw new IllegalArgumentException
    }
  }

}

