package lesson4

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.{Vector, VectorUDTPublic, Vectors}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

object Main {
  private val log = LoggerFactory.getLogger(getClass)
  private val labelCol = "label"

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("Iablokov Lesson 4").master("local[1]").getOrCreate()
    val vectorsRdd: RDD[Vector] = readVectors(ss)
    val labelsRdd: RDD[Int] = readLabels(ss)
    val labelledVectorsRdd = vectorsRdd.zip(labelsRdd).map(tuple => Row(tuple._1, tuple._2))
    val labelledVectorsDf: DataFrame = rddToDf(ss, labelledVectorsRdd)
    val (trainingData: Dataset[Row], testData: Dataset[Row]) = splitInputData(labelledVectorsDf)
    val model: LinearRegressionModel = fitModel(trainingData)
    printSummary(model)
    evaluate(testData, model)
    ss.close
  }

  private def fitModel(trainingData: Dataset[Row]) = {
    val maxIter = 10
    val estimator = new LinearRegression().setMaxIter(maxIter)
    val model = estimator.fit(trainingData)
    model
  }

  private def evaluate(testData: Dataset[Row], model: LinearRegressionModel) = {
    val predictions = model.transform(testData)
    predictions.show(100)
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println(rmse)
  }

  private def splitInputData(labelledVectorsDf: DataFrame) = {
    val labelledVectors = labelledVectorsDf.randomSplit(Array[Double](0.5, 0.5), 1L)
    assert(labelledVectors.length == 2)
    val trainingData = labelledVectors(0)
    val testData = labelledVectors(1)
    println("Input data size = " + labelledVectorsDf.count)
    println("Test data size = " + testData.count)
    println("Training data size = " + trainingData.count)
    (trainingData, testData)
  }

  private def rddToDf(ss: SparkSession, labelledVectorsRdd: RDD[Row]) = {
    val featuresCol = "features"
    val schema = StructType(
      StructField(featuresCol, VectorUDTPublic, nullable = false) ::
        StructField(labelCol, IntegerType, nullable = false) :: Nil
    )

    val labelledVectorsDf = ss.createDataFrame(labelledVectorsRdd, schema)
    labelledVectorsDf.show
    labelledVectorsDf
  }

  private def readLabels(ss: SparkSession) = {
    val labelsPath = resourceToPath("Target.csv")
    val labelsRdd = ss.sparkContext.textFile(labelsPath).map(_.toInt)
    assert(labelsRdd.count() == 15223)
    labelsRdd
  }

  private def readVectors(ss: SparkSession) = {
    val vectorsPath = resourceToPath("Objects.csv")
    val vectorsRdd = ss.sparkContext.textFile(vectorsPath)
      .map(line => line.replaceAll(",", "."))
      .map(line => line.split(";").map(value => value.toDouble).map(value => if (value.isNaN) 0 else value).array)
      .map(array => Vectors.dense(array))
    assert(vectorsRdd.count() == 15223)
    vectorsRdd
  }

  private def printSummary(model: LinearRegressionModel) = {
    val trainingSummary = model.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
    trainingSummary.residuals.show()
  }

  private def resourceToPath(resource: String) = {
    val url = getClass.getClassLoader.getResource(resource)
    if (url == null) {
      throw new RuntimeException("Resource not found: " + resource)
    }
    val path = url.toString
    log.info("Path to resource: " + path)
    path
  }
}