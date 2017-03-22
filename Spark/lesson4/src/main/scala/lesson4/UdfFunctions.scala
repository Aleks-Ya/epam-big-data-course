package lesson4

import org.apache.spark.ml.linalg.Vectors

import scala.collection.mutable.ListBuffer
import scala.util.Try

object UdfFunctions {
  val numericalToRawFeatures: DescriptionParser => Seq[String] => Seq[Double] = parser => x => {
    val result = new ListBuffer[Double]()
    parser.numericFields.map({ t =>
      val id = t._1.toInt - 1
      val valueStr = x(id)
      var value = Try(valueStr.toDouble).getOrElse({
        //          log.warn(s"Can't parse Int: $valueStr. Use 0")
        0d
      })
      if (value.isNaN) {
        //          log.warn("value is NaN. Use 0")
        value = 0d
      }
      result += value
    })
    result.toList
  }

  val categoricalObjectToCategorical: String => Seq[String] => Int = column => objects => {
    //      assert(objects.size == fieldsCount, objects.size)
    val fieldId: Int = extractIdFromColumnName(column)
    val valueStr = objects(fieldId)
    val value = Try(valueStr.toInt).getOrElse({
      //        log.warn(s"Can't parse Int: $valueStr. Use 0")
      0
    })
    if (value.isNaN) throw new RuntimeException("NAN")
    value
  }

  def extractIdFromColumnName(column: String): Int = {
    val fieldId = column.split("_")(1).toInt - 1
    fieldId
  }

  val appendRawCategoricalToRawFeatures: (org.apache.spark.ml.linalg.Vector, Seq[Double]) => Seq[Double] = (vector, rawFeatures) => {
    val res = ListBuffer[Double]()
    res ++= vector.toDense.toArray
    res ++= rawFeatures
    res.toList
  }

  val rawFeaturesToVector: Seq[Double] => org.apache.spark.ml.linalg.Vector = rawFeatures => {
    Vectors.dense(rawFeatures.toArray[Double])
  }


}
