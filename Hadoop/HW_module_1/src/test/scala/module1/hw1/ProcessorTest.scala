package module1.hw1

import org.scalatest.{FlatSpec, Matchers}

class ProcessorTest extends FlatSpec with Matchers {

  "Processor" should "count top 3" in {
    val files = List(
      getClass.getResourceAsStream("processor1.txt"),
      getClass.getResourceAsStream("processor2.txt")
    )
    val top3 = Processor.process(files, 3, 3)
    top3.toString shouldEqual "List((20130612000102824,5), (20130612000102843,4), (20130612000102832,3))"
  }

}

