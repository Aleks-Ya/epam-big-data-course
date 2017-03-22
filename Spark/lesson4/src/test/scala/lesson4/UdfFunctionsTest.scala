package lesson4

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.{FlatSpec, Matchers}

class UdfFunctionsTest extends FlatSpec with Matchers {
  it should "select values from all fields only numerical" in {
    val content = FileHelper.readDescriptions(SparkHelper.ss)
    val parser = new DescriptionParser(content)
    val objects = (for (i <- 101 to 151) yield i).map(n => n.toString)
    val res = UdfFunctions.numericalToRawFeatures(parser)(objects)
    println(res)
    res should have size 34
    res should contain allOf(
      101.0, 105.0, 106.0, 115.0, 121.0, 122.0, 123.0, 124.0, 125.0, 126.0,
      127.0, 128.0, 129.0, 130.0, 131.0, 132.0, 133.0, 134.0, 135.0, 136.0,
      137.0, 138.0, 139.0, 140.0, 141.0, 142.0, 143.0, 144.0, 145.0, 146.0,
      147.0, 148.0, 149.0, 150.0)
  }

  it should "return value of categorical field for this column" in {
    val column = "column_2"
    val objects = (for (i <- 101 to 151) yield i).map(n => n.toString)
    val res = UdfFunctions.categoricalObjectToCategorical(column)(objects)
    res shouldEqual 102
  }

  it should "append value of raw categorical feature to all features" in {
    val vector = Vectors.dense(1d, 2d)
    val s = Seq(3d, 4d)
    val res = UdfFunctions.appendRawCategoricalToRawFeatures(vector, s)
    res should contain inOrder(1, 2, 3, 4)
  }

  it should "cover array to Vector" in {
    val s = Seq(1d, 2d)
    val res = UdfFunctions.rawFeaturesToVector(s)
    res shouldEqual Vectors.dense(1d, 2d)
  }
}
