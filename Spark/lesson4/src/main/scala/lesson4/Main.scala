package lesson4

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._

object Main {
  private val log = LoggerFactory.getLogger(getClass)
  private val labelCol = "labels"
  private val objectsCol = "objects"
  private val rawFeaturesCol = "rawFeatures"
  private val rawCategoricalCol = "rawCategorical"
  private val featuresCol = "features"

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("Iablokov Lesson 4").master("local[1]").getOrCreate()
    val objectsRdd: RDD[Array[String]] = readObjects(ss)
    val labelsRdd: RDD[Int] = readLabels(ss)
    val labelObjectRdd = objectsRdd.zip(labelsRdd).map(tuple => Row(tuple._1, tuple._2))
    var labelObjectDf: DataFrame = rddToDf(ss, labelObjectRdd)
      .withColumn(rawFeaturesCol, array())

    DescriptionParser.content = readDescriptions(ss)
    assert(DescriptionParser.allFields.size == 51)
    DescriptionParser.categoricalFields.foreach { t =>
      val id = t._1
      labelObjectDf = labelObjectDf.withColumn("rawCategorical_" + id, lit(-1))
    }
    labelObjectDf.show
//    labelObjectDf.foreach(objectRow => {
//      val fields = objectRow.getSeq[String](0)
//      val rawFeatures = objectRow.getSeq[Double](2)
//      fields.zipWithIndex.foreach(tuple => {
//        val index = tuple._2
//        val value = tuple._1
//        val description = DescriptionParser.allFields(index)
//        description.category match {
//          case Category.Numeric => {
//            rawFeatures :+ value.toDouble
//          }
//          case Category.Categorical => labelObjectDf.col("rawCategorical_" + description.id)
//        }
//      })
//    })


    //    DescriptionParser.allFields.foreach(tuple => {
    //
    //    })
    //    DescriptionParser.numericFields.foreach(tuple => {
    //      labelObjectDf.withColumn("feature_" + tuple._1, lit(-1))
    //    })
    //    DescriptionParser.categoricalFields.foreach(tuple => {
    //      labelObjectDf.withColumn("feature_" + tuple._1, lit())
    //    })


    //    labelObjectDf.map(row => {
    //      val objects = row.getAs[Array[String]](objectsCol)
    //      objects.zipWithIndex.foreach(tuple => {
    //        val index = tuple._2
    //        val value = tuple._1
    //        val description = DescriptionParser.allFields(index)
    //
    //      })
    //    })


    //    val (trainingData: Dataset[Row], testData: Dataset[Row]) = splitInputData(labelObjectDf)
    //    val model: LinearRegressionModel = fitModel(trainingData)
    //    printSummary(model)
    //    evaluate(testData, model)
    ss.close
  }

  private def objectToVector(obj: Array[String]) = {
    obj.zipWithIndex.foreach(tuple => {
      val index = tuple._2
      val value = tuple._1
      val description = DescriptionParser.allFields(index)
      description match {
        case Category.Numeric => value.toDouble
        case Category.Categorical =>
      }
      index match {
        case 0 => value.toInt
        case _ => throw new IllegalArgumentException("Unexpected index: " + index)
      }
    })
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
    val schema = StructType(
      StructField(objectsCol, ArrayType(StringType), nullable = false) ::
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

  private def readObjects(ss: SparkSession) = {
    val vectorsPath = resourceToPath("Objects.csv")
    val vectorsRdd = ss.sparkContext.textFile(vectorsPath)
      .map(line => line.replaceAll(",", "."))
      .map(line => line.split(";"))
    assert(vectorsRdd.count() == 15223)
    vectorsRdd
  }

  private def readDescriptions(ss: SparkSession) = {
    val path = resourceToPath("PropertyDesciptionEN.txt")
    ss.sparkContext.textFile(path).reduce(_ + "\n" + _)
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