package lesson4

import org.scalatest.{FlatSpec, Matchers}

class DescriptionParserTest extends FlatSpec with Matchers {
  it should "parse" in {
    val content =
      """2) Is employed: CATEGORICAL
        |	0: employed/yet-already
        |	1: not employed""".stripMargin
    DescriptionParser.content = content
    val description = DescriptionParser.allFields(2)
    description.id shouldEqual 2
    description.title shouldEqual "Is employed"
    description.category shouldEqual Category.Categorical
    description.values should have size 2
    description.values.head._1 shouldEqual 0
    description.values.head._2 shouldEqual "employed/yet-already"
    description.values(1)._1 shouldEqual 1
    description.values(1)._2 shouldEqual "not employed"
  }
}
