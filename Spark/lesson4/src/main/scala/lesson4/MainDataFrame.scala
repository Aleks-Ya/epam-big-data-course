package lesson4

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.{VectorUDTPublic, Vectors}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

object MainDataFrame {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .appName("Iablokov Lesson 4")
      .master("local[*]")
      .getOrCreate()

    val inputDataSize = 15223
    val labelCol = "label"
    val idCol = "id"
    val featuresCol = "features"

    val vectorsPath = resourceToPath("Objects.csv")
    val vectorsRdd = ss.sparkContext.textFile(vectorsPath)
      .map(line => line.replaceAll(",", "."))
      .map(line => line.split(";").map(value => value.toDouble).array)
      .map(array => Vectors.dense(array))
      .map(vector => Row(vector))
    assert(vectorsRdd.count() == inputDataSize)

    val schema = StructType(
      StructField(featuresCol, VectorUDTPublic, nullable = false) :: Nil
    )

    val vectorsDf = ss.createDataFrame(vectorsRdd, schema)
      .withColumn(idCol, monotonically_increasing_id())

    val labelsPath = resourceToPath("Target.csv")
    val labelsDf = ss.read.csv(labelsPath)
      .withColumnRenamed("_c0", labelCol)
      .withColumn(labelCol, col(labelCol).cast(IntegerType))
      .withColumn(idCol, monotonically_increasing_id())
    assert(labelsDf.count() == inputDataSize)

    val labelledVectors = vectorsDf.join(labelsDf, idCol).randomSplit(Array[Double](1, 1))
    val trainingData = labelledVectors(0)
    val testData = labelledVectors(1)

    val estimator = new LinearRegression().setMaxIter(10)
    val model = estimator.fit(trainingData)

    val trainingSummary = model.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
    trainingSummary.residuals.show()

    val predictions = model.transform(testData)
    predictions.show
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println(rmse)

    ss.close
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