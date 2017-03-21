package lesson4

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{LabeledPoint, OneHotEncoder}
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
  private val labelCol = "labels"
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

    labelObjectDf.foreach(row => {
      val l = row.getSeq(0).length
      assert(l == fieldsCount, l + "-" + row)
    })

    DescriptionParser.content = readDescriptions(ss)
    assert(DescriptionParser.allFields.size == fieldsCount)
    assert(DescriptionParser.categoricalFields.size == 16)
    assert(DescriptionParser.numericFields.size == 34)

    labelObjectDf = addCategoricalColumn(labelObjectDf)
    labelObjectDf.show

    labelObjectDf = numericalToRawFeatures(labelObjectDf)
    labelObjectDf.show

    labelObjectDf = categoricalObjectToCategorical(labelObjectDf)
    labelObjectDf.show

    labelObjectDf = transformCategoricalToRawCategorical(labelObjectDf)
    labelObjectDf.show

    labelObjectDf = appendRawCategoricalToRawFeatures(labelObjectDf)
    labelObjectDf.show

    labelObjectDf = rawFeaturesToLabelledPoint(labelObjectDf)
    labelObjectDf.show

    val (trainingData: Dataset[Row], testData: Dataset[Row]) = splitInputData(labelObjectDf)
    val model: LinearRegressionModel = fitModel(trainingData)
    printSummary(model)
    evaluate(testData, model)
    ss.close
  }

  private def initSparkSession = {
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

  private def addCategoricalColumn(labelObjectDf1: DataFrame) = {
    var labelObjectDf = labelObjectDf1
    DescriptionParser.categoricalFields.foreach { t =>
      val id = t._1.toInt - 1
      val colName = categoricalPrefix + id
      labelObjectDf = labelObjectDf.withColumn(colName, lit(-1))
    }
    labelObjectDf
  }

  private def numericalToRawFeatures(labelObjectDf: DataFrame) = {
    val fillNumericalCols: Seq[String] => Seq[Double] = (x) => {
      val result = new ListBuffer[Double]()
      DescriptionParser.numericFields.map({ t =>
        val id = t._1.toInt - 1
        val valueStr = x(id)
        val value = Try(valueStr.toDouble).getOrElse({
          log.warn(s"Can't parse Int: $valueStr. Use 0")
          0d
        })
        result += value
      })
      result
    }

    val fillNumericalColsUdf = udf(fillNumericalCols)

    labelObjectDf.withColumn(rawFeaturesCol, fillNumericalColsUdf(col(objectsCol)))
  }

  private def categoricalObjectToCategorical(labelObjectDf1: DataFrame) = {
    var labelObjectDf = labelObjectDf1

    val objToCategorical: String => Seq[String] => Int = column => objects => {
      assert(objects.size == fieldsCount, objects.size)
      val fieldId: Int = extractIdFromColumnName(column)
      val valueStr = objects(fieldId)
      val value = Try(valueStr.toInt).getOrElse({
        log.warn(s"Can't parse Int: $valueStr. Use 0")
        0
      })
      value
    }

    labelObjectDf.columns.filter(col => col.startsWith(categoricalPrefix)).foreach(column => {
      val fillCategoricalColsUdf = udf(objToCategorical(column))
      labelObjectDf = labelObjectDf.withColumn(column, fillCategoricalColsUdf(col(objectsCol)))
    })
    labelObjectDf
  }

  private def transformCategoricalToRawCategorical(labelObjectDf1: DataFrame) = {
    var labelObjectDf = labelObjectDf1
    labelObjectDf.columns.filter(col => col.startsWith(categoricalPrefix)).foreach(column => {
      val fieldId: Int = extractIdFromColumnName(column)
      val rawColumn = rawCategoricalPrefix + fieldId
      log.info(s"InputCol: $column, OutputCol: $rawColumn")
      val encoder = new OneHotEncoder()
        .setInputCol(column)
        .setOutputCol(rawColumn)
      labelObjectDf = encoder.transform(labelObjectDf)
    })
    labelObjectDf
  }

  private def extractIdFromColumnName(column: String) = {
    val fieldId = column.split("_")(1).toInt - 1
    fieldId
  }

  private def appendRawCategoricalToRawFeatures(labelObjectDf1: DataFrame) = {
    var labelObjectDf = labelObjectDf1
    labelObjectDf = labelObjectDf.withColumn(rawCategoricalCol, lit(Array[Int]()))
    val fillCategoricalCols: String => (SparseVector, Seq[Double]) => Seq[Double] = column => (vector, rawFeatures) => {
      rawFeatures.toBuffer ++= vector.toDense.toArray
    }

    var result = labelObjectDf
    labelObjectDf.columns.filter(col => col.startsWith(rawCategoricalPrefix)).foreach(column => {
      val fillCategoricalColsUdf = udf(fillCategoricalCols(column))
      result = result.withColumn(rawFeaturesCol, fillCategoricalColsUdf(col(column), col(rawFeaturesCol)))
    })
    result
  }

  private def rawFeaturesToLabelledPoint(labelObjectDf1: DataFrame) = {
    var labelObjectDf = labelObjectDf1


    val udfFun: (Seq[Double], Int) => LabeledPoint = (rawFeatures, label) => {
      LabeledPoint(label, Vectors.dense(rawFeatures.toArray))
    }
    val udfObj = udf(udfFun)
    labelObjectDf = labelObjectDf.withColumn(featuresCol, udfObj(col(rawFeaturesCol), col(labelCol)))
    labelObjectDf
  }

  //  private def objectToVector(obj: Array[String])
  //
  //  = {
  //    obj.zipWithIndex.foreach(tuple => {
  //      val index = tuple._2
  //      val value = tuple._1
  //      val description = DescriptionParser.allFields(index)
  //      description match {
  //        case Category.Numeric => value.toDouble
  //        case Category.Categorical =>
  //      }
  //      index match {
  //        case 0 => value.toInt
  //        case _ => throw new IllegalArgumentException("Unexpected index: " + index)
  //      }
  //    })
  //  }

  private def fitModel(trainingData: Dataset[Row])

  = {
    val maxIter = 10
    val estimator = new LinearRegression().setMaxIter(maxIter)
    val model = estimator.fit(trainingData)
    model
  }

  private def evaluate(testData: Dataset[Row], model: LinearRegressionModel)

  = {
    val predictions = model.transform(testData)
    predictions.show(100)
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println(rmse)
  }

  private def splitInputData(labelledVectorsDf: DataFrame)

  = {
    val labelledVectors = labelledVectorsDf.randomSplit(Array[Double](0.5, 0.5), 1L)
    assert(labelledVectors.length == 2)
    val trainingData = labelledVectors(0)
    val testData = labelledVectors(1)
    println("Input data size = " + labelledVectorsDf.count)
    println("Test data size = " + testData.count)
    println("Training data size = " + trainingData.count)
    (trainingData, testData)
  }

  private def rddToDf(ss: SparkSession, labelledVectorsRdd: RDD[Row])

  = {
    val schema = StructType(
      StructField(objectsCol, ArrayType(StringType), nullable = false) ::
        StructField(labelCol, IntegerType, nullable = false) :: Nil
    )

    val labelledVectorsDf = ss.createDataFrame(labelledVectorsRdd, schema)
    labelledVectorsDf.show
    labelledVectorsDf
  }

  private def readLabels(ss: SparkSession)

  = {
    val labelsPath = resourceToPath("Target.csv")
    val labelsRdd = ss.sparkContext.textFile(labelsPath).map(_.toInt)
    assert(labelsRdd.count() == 15223)
    labelsRdd
  }

  private def readObjects(ss: SparkSession)

  = {
    val vectorsPath = resourceToPath("Objects.csv")
    val vectorsRdd = ss.sparkContext.textFile(vectorsPath)
      .map(line => line.replaceAll(",", "."))
      .map(line => line.split(";"))
    assert(vectorsRdd.count() == 15223)
    vectorsRdd.zipWithIndex().foreach(t => {
      val l = t._1.length
      assert(l == fieldsCount, s"$l-${t._2}-${t._1.toList}")
    })
    vectorsRdd
  }

  private def readDescriptions(ss: SparkSession)

  = {
    val path = resourceToPath("PropertyDesciptionEN.txt")
    ss.sparkContext.textFile(path).reduce(_ + "\n" + _)
  }

  private def printSummary(model: LinearRegressionModel)

  = {
    val trainingSummary = model.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
    trainingSummary.residuals.show()
  }

  private def resourceToPath(resource: String)

  = {
    val url = getClass.getClassLoader.getResource(resource)
    if (url == null) {
      throw new RuntimeException("Resource not found: " + resource)
    }
    val path = url.toString
    log.info("Path to resource: " + path)
    path
  }
}