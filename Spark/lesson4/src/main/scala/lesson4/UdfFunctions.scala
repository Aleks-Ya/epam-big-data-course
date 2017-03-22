package lesson4

import org.apache.spark.ml.linalg.{SparseVector, Vectors}

import scala.collection.mutable.ListBuffer
import scala.util.Try

object UdfFunctions {
  val numericalToRawFeatures: Seq[String] => Seq[Double] = (x) => {
    val result = new ListBuffer[Double]()
    DescriptionParser.numericFields.map({ t =>
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
    result
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

  val appendRawCategoricalToRawFeatures: String => (SparseVector, Seq[Double]) => Seq[Double] = column => (vector, rawFeatures) => {
    rawFeatures.toBuffer ++= vector.toDense.toArray
  }

  val rawFeaturesToLabelledPoint: (Seq[Double], Int) => org.apache.spark.ml.linalg.Vector = (rawFeatures, label) => {
    Vectors.dense(rawFeatures.toArray[Double])
  }


}
