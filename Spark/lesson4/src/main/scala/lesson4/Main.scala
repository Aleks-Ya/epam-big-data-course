package lesson4

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.linalg.{VectorUDTPublic, Vectors}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.util.Try

object Main {
  private val log = LoggerFactory.getLogger(getClass)
  private val labelCol = "label"
  private val featuresCol = "features"
  private val categoricalCol = "categorical"
  private val categoricalVectorCol = "categoricalVector"
  private val objectIdCol = "objectId"

  def main(args: Array[String]): Unit = {
    val ss = SparkHelper.ss

    val descriptions = new DescriptionParser(FileHelper.readDescriptions)

    val objectsRdd: RDD[(Long, Array[String])] = FileHelper.readObjects(ss).zipWithIndex.map(_.swap)

    var featureRdd: RDD[(Long, Array[Double])] = objectsRdd.map(t => {
      val objId = t._1
      val objValues = t._2
      val features = objValues.zipWithIndex
        .filter(t => descriptions.numericFields.contains(t._2))
        .map(t => parseDouble(t._1))
      (objId, features)
    })
    featureRdd.take(10).foreach(t => println(t._1 + " - " + t._2.toList))

    val schema = StructType(
      StructField(objectIdCol, LongType) ::
        StructField(categoricalCol, DoubleType) :: Nil)

    descriptions.categoricalFields.foreach(fieldTuple => {
      val categoricalRdd = objectsRdd.map(objTuple => {
        val objId = objTuple._1
        val fieldId = fieldTuple._1
        val fieldValue = objTuple._2.zipWithIndex.map(_.swap).toMap.get(fieldId).get
        val double = parseDouble(fieldValue)
        Row(objId, double)
      })
      val df = SparkHelper.ss.createDataFrame(categoricalRdd, schema)
      val encoder = new OneHotEncoder().setInputCol(categoricalCol).setOutputCol(categoricalVectorCol)
      val transformedDf = encoder.transform(df)
      transformedDf.show

      implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[(Long, org.apache.spark.ml.linalg.Vector)]
      val categoricalVectorRdd = transformedDf.map(row => {
        val objectIdColInx = row.fieldIndex(objectIdCol)
        val categoricalVectorColIdx = row.fieldIndex(categoricalVectorCol)
        (row.getLong(objectIdColInx), row.getAs[org.apache.spark.ml.linalg.Vector](categoricalVectorColIdx))
      }).rdd

      featureRdd = featureRdd.zip(categoricalVectorRdd).map(t => {
        assert(t._1._1 == t._2._1)
        val objId = t._1._1
        val featureArr = t._1._2.toBuffer
        val joinedFeatures = featureArr ++= t._2._2.toArray
        (objId, joinedFeatures.toArray)
      })
    })

    val vectorRdd: RDD[(Long, org.apache.spark.ml.linalg.Vector)] = featureRdd.map(t => (t._1, Vectors.dense(t._2)))
//    vectorRdd.take(5).foreach(t => println(t._1 + " - " + t._2))

    val labelsRdd: RDD[(Long, Int)] = FileHelper.readLabels(ss).zipWithIndex().map(_.swap)

    val labelVectorRdd = vectorRdd.join(labelsRdd).map(t => Row(t._2._2, t._2._1))

    val schema2 = StructType(
      StructField(labelCol, IntegerType) ::
        StructField(featuresCol, VectorUDTPublic) :: Nil)


    val labelVectorDf = SparkHelper.ss.createDataFrame(labelVectorRdd, schema2)

    val (trainingData: Dataset[Row], testData: Dataset[Row]) = splitInputData(labelVectorDf)
    val model: LinearRegressionModel = fitModel(trainingData)
    printSummary(model)
    evaluate(testData, model)
    ss.close
  }

  private def fitModel(trainingData: Dataset[Row]) = {
    log.info("Enter fitModel")
    val maxIter = 100
    val estimator = new LinearRegression().setMaxIter(maxIter)
    estimator.fit(trainingData)
  }

  private def evaluate(testData: Dataset[Row], model: LinearRegressionModel) = {
    log.info("Enter evaluate")
    val predictions = model.transform(testData)
    predictions.show(100)
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    log.info(rmse.toString)
  }

  private def splitInputData(labelledVectorsDf: DataFrame) = {
    log.info("Enter splitInputData")
    val labelledVectors = labelledVectorsDf.randomSplit(Array[Double](0.5, 0.5), 1L)
    //    assert(labelledVectors.length == 2)
    val trainingData = labelledVectors(0)
    val testData = labelledVectors(1)
    //    log.info("Input data size = " + labelledVectorsDf.count)
    //    log.info("Test data size = " + testData.count)
    //    log.info("Training data size = " + trainingData.count)
    (trainingData, testData)
  }

  private def printSummary(model: LinearRegressionModel) = {
    log.info("Enter printSummary")
    val trainingSummary = model.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
    trainingSummary.residuals.show()
  }

  def parseDouble(s: String): Double = {
    var value = Try(s.toDouble).getOrElse({
      //          log.warn(s"Can't parse Int: $valueStr. Use 0")
      0d
    })
    if (value.isNaN) {
      //          log.warn("value is NaN. Use 0")
      value = 0d
    }
    value
  }

}