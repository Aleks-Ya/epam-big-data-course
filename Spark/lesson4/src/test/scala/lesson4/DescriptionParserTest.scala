package lesson4

import org.scalatest.{FlatSpec, Matchers}

class DescriptionParserTest extends FlatSpec with Matchers {
  it should "parse categorical" in {
    val content =
      """39) Is employed, USD: CATEGORICAL
        |	0: employed/yet-al,re<ady
        |	1: not employed""".stripMargin
    DescriptionParser.content = content
    val description = DescriptionParser.allFields(39)
    description.id shouldEqual 39
    description.title shouldEqual "Is employed, USD"
    description.category shouldEqual Category.Categorical
    description.values should have size 2
    description.values.head._1 shouldEqual 0
    description.values.head._2 shouldEqual "employed/yet-al,re<ady"
    description.values(1)._1 shouldEqual 1
    description.values(1)._2 shouldEqual "not employed"
  }


//  it should "parse numeric" in {
//    val content =
//      """48) Average amount of the delayed payment, USD: NUMERIC"""
//    DescriptionParser.content = content
//    val description = DescriptionParser.allFields(48)
//    description.id shouldEqual 48
//    description.title shouldEqual "Average amount of the delayed payment, USD"
//    description.category shouldEqual Category.Numeric
//    description.values should have size 0
//  }
}
