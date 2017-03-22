package lesson4

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.util.Try

object Main {
  private val log = LoggerFactory.getLogger(getClass)
  private val labelCol = "label"
  private val objectsCol = "objects"
  private val rawFeaturesCol = "rawFeatures"
  private val rawCategoricalCol = "rawCategorical"
  private val featuresCol = "features"

  private val fieldsCount = 50

  private val categoricalPrefix = "categorical_"
  private val rawCategoricalPrefix = "rawCategorical_"

  def main(args: Array[String]): Unit = {
    val ss = initSparkSession

    val objectsRdd: RDD[Array[String]] = readObjects(ss)
    val labelsRdd: RDD[Int] = readLabels(ss)
    val labelObjectRdd = objectsRdd.zip(labelsRdd).map(tuple => Row(tuple._1, tuple._2))
    var labelObjectDf: DataFrame = rddToDf(ss, labelObjectRdd)
      .withColumn(rawFeaturesCol, array())

//    labelObjectDf.foreach(row => {
//      val l = row.getSeq(0).length
//      assert(l == fieldsCount, l + "-" + row)
//    })

    DescriptionParser.content = readDescriptions(ss)
//    assert(DescriptionParser.allFields.size == fieldsCount)
//    assert(DescriptionParser.categoricalFields.size == 16)
//    assert(DescriptionParser.numericFields.size == 34)

    labelObjectDf = addCategoricalColumns(labelObjectDf)


    labelObjectDf = numericalToRawFeatures(labelObjectDf)
    //    labelObjectDf.cache
//    labelObjectDf.show

    labelObjectDf = categoricalObjectToCategorical(labelObjectDf)
    //    labelObjectDf.cache
//    labelObjectDf.show

    labelObjectDf = transformCategoricalToRawCategorical(labelObjectDf)
    //    labelObjectDf.cache
//    labelObjectDf.show

    labelObjectDf = appendRawCategoricalToRawFeatures(labelObjectDf)
    //    labelObjectDf.cache
//    labelObjectDf.show

    labelObjectDf = rawFeaturesToLabelledPoint(labelObjectDf)
    //    labelObjectDf.cache
//    labelObjectDf.show

    labelObjectDf = dropUnusedColumns(labelObjectDf)
    log.info("Start to cache")

    labelObjectDf.cache
    labelObjectDf.explain(extended = true)
//    labelObjectDf.show

    val (trainingData: Dataset[Row], testData: Dataset[Row]) = splitInputData(labelObjectDf)
    //    trainingData.show(100, truncate = false)
    val model: LinearRegressionModel = fitModel(trainingData)
    printSummary(model)
    evaluate(testData, model)
    ss.close
  }

  private def dropUnusedColumns(labelObjectDf1: DataFrame) = {
    log.info("Enter dropUnusedColumns")
    var labelObjectDf = labelObjectDf1
    labelObjectDf.columns
      .filter(col => col.startsWith(categoricalPrefix) || col.startsWith(rawCategoricalPrefix))
      .foreach(column => labelObjectDf = labelObjectDf.drop(col(column)))
    labelObjectDf = labelObjectDf.drop(col(objectsCol))
    labelObjectDf = labelObjectDf.drop(col(rawFeaturesCol))
    labelObjectDf = labelObjectDf.drop(col(rawCategoricalCol))
    labelObjectDf
  }


  private def initSparkSession = {
    //TODO use >1 cores
    val builder = SparkSession.builder().appName("Iablokov Lesson 4").master("local[1]")

    val logDir = sys.env.get("SPARK_HISTORY_FS_LOG_DIRECTORY")
    if (logDir.isDefined) {
      builder
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", logDir.get)
      println("Set event log dir: " + logDir.get)
    }
    val ss = builder.getOrCreate()
    ss
  }

  private def addCategoricalColumns(labelObjectDf1: DataFrame) = {
    log.info("Enter addCategoricalColumns")
    var labelObjectDf = labelObjectDf1
    DescriptionParser.categoricalFields.foreach { t =>
      val id = t._1.toInt - 1
      val colName = categoricalPrefix + id
      labelObjectDf = labelObjectDf.withColumn(colName, lit(-1))
    }
//    labelObjectDf.show
    labelObjectDf
  }

  private def numericalToRawFeatures(labelObjectDf: DataFrame) = {
    log.info("Enter numericalToRawFeatures")
    val fillNumericalColsUdf = udf(UdfFunctions.numericalToRawFeatures)
    labelObjectDf.withColumn(rawFeaturesCol, fillNumericalColsUdf(col(objectsCol)))
  }

  private def categoricalObjectToCategorical(labelObjectDf1: DataFrame) = {
    log.info("Enter categoricalObjectToCategorical")
    var labelObjectDf = labelObjectDf1

    labelObjectDf.columns.filter(col => col.startsWith(categoricalPrefix)).foreach(column => {
      val fillCategoricalColsUdf = udf(UdfFunctions.categoricalObjectToCategorical(column))
      labelObjectDf = labelObjectDf.withColumn(column, fillCategoricalColsUdf(col(objectsCol)))
    })
    labelObjectDf
  }

  private def transformCategoricalToRawCategorical(labelObjectDf1: DataFrame) = {
    log.info("Enter transformCategoricalToRawCategorical")
    var labelObjectDf = labelObjectDf1
    labelObjectDf.columns.filter(col => col.startsWith(categoricalPrefix)).foreach(column => {
      val fieldId: Int = UdfFunctions.extractIdFromColumnName(column)
      val rawColumn = rawCategoricalPrefix + fieldId
      val encoder = new OneHotEncoder()
        .setInputCol(column)
        .setOutputCol(rawColumn)
      labelObjectDf = encoder.transform(labelObjectDf)
    })
    labelObjectDf
  }

  private def appendRawCategoricalToRawFeatures(labelObjectDf1: DataFrame) = {
    log.info("Enter appendRawCategoricalToRawFeatures")
    var labelObjectDf = labelObjectDf1
    labelObjectDf = labelObjectDf.withColumn(rawCategoricalCol, lit(Array[Int]()))

    var result = labelObjectDf
    labelObjectDf.columns.filter(col => col.startsWith(rawCategoricalPrefix)).foreach(column => {
      val fillCategoricalColsUdf = udf(UdfFunctions.appendRawCategoricalToRawFeatures(column))
      result = result.withColumn(rawFeaturesCol, fillCategoricalColsUdf(col(column), col(rawFeaturesCol)))
    })
    result
  }

  private def rawFeaturesToLabelledPoint(labelObjectDf1: DataFrame) = {
    log.info("Enter rawFeaturesToLabelledPoint")
    var labelObjectDf = labelObjectDf1
    val udfObj = udf(UdfFunctions.rawFeaturesToLabelledPoint)
    labelObjectDf = labelObjectDf.withColumn(featuresCol, udfObj(col(rawFeaturesCol), col(labelCol)))
    labelObjectDf
  }

  private def fitModel(trainingData: Dataset[Row]) = {
    log.info("Enter fitModel")
    val maxIter = 10
    val estimator = new LinearRegression().setMaxIter(maxIter)
    estimator.fit(trainingData)
  }

  private def evaluate(testData: Dataset[Row], model: LinearRegressionModel) = {
    log.info("Enter evaluate")
    val predictions = model.transform(testData)
//    predictions.show(100)
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

  private def rddToDf(ss: SparkSession, labelledVectorsRdd: RDD[Row]) = {
    log.info("Enter rddToDf")
    val schema = StructType(
      StructField(objectsCol, ArrayType(StringType), nullable = false) ::
        StructField(labelCol, IntegerType, nullable = false) :: Nil
    )

    val labelledVectorsDf = ss.createDataFrame(labelledVectorsRdd, schema)
//    labelledVectorsDf.show
    labelledVectorsDf
  }

  private def readLabels(ss: SparkSession) = {
    log.info("Enter readLabels")
    val labelsPath = resourceToPath("Target.csv")
    val labelsRdd = ss.sparkContext.textFile(labelsPath).map(_.toInt)
    //    assert(labelsRdd.count() == 15223)
    labelsRdd
  }

  private def readObjects(ss: SparkSession) = {
    log.info("Enter readObjects")
    val vectorsPath = resourceToPath("Objects.csv")
    val vectorsRdd = ss.sparkContext.textFile(vectorsPath)
      .map(line => line.replaceAll(",", "."))
      .map(line => line.split(";"))
    //    assert(vectorsRdd.count() == 15223)
//    vectorsRdd.zipWithIndex().foreach(t => {
//      val l = t._1.length
//      assert(l == fieldsCount, s"$l-${t._2}-${t._1.toList}")
//    })
    vectorsRdd
  }

  private def readDescriptions(ss: SparkSession) = {
    log.info("Enter readDescriptions")
    val path = resourceToPath("PropertyDesciptionEN.txt")
    ss.sparkContext.textFile(path).reduce(_ + "\n" + _)
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

  private def resourceToPath(resource: String) = {
    log.info("Enter resourceToPath")
    val url = getClass.getClassLoader.getResource(resource)
    if (url == null) {
      throw new RuntimeException("Resource not found: " + resource)
    }
    val path = url.toString
    log.info("Path to resource: " + path)
    path
  }
}