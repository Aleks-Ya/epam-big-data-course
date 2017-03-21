package lesson4

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
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

  private val nanValue = 0

  private val fieldsCount = 50

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("Iablokov Lesson 4").master("local[1]").getOrCreate()
    val objectsRdd: RDD[Array[String]] = readObjects(ss)
    val labelsRdd: RDD[Int] = readLabels(ss)
    val labelObjectRdd = objectsRdd.zip(labelsRdd).map(tuple => Row(tuple._1, tuple._2))
    var labelObjectDf: DataFrame = rddToDf(ss, labelObjectRdd)
      .withColumn(rawFeaturesCol, array())

    labelObjectDf.foreach(row => {
      val l = row.getSeq(nanValue).length
      assert(l == fieldsCount, l + "-" + row)
    })

    DescriptionParser.content = readDescriptions(ss)
    assert(DescriptionParser.allFields.size == fieldsCount)
    assert(DescriptionParser.categoricalFields.size == 16)
    assert(DescriptionParser.numericFields.size == 34)

    DescriptionParser.categoricalFields.foreach { t =>
      val id = t._1.toInt - 1
      log.info("Categorical id: " + id)
      val colName = "rawCategorical_" + id
      log.info("create column: " + colName)
      labelObjectDf = labelObjectDf.withColumn(colName, lit(-1))
    }
    labelObjectDf.show

    labelObjectDf = numericalToRawFeatures(labelObjectDf)
    labelObjectDf.show

    labelObjectDf = rawCategoricalToRawFeatures(labelObjectDf)
    labelObjectDf.show

    labelObjectDf.select(col(rawFeaturesCol)).take(10).foreach(println)


    //    val splitFields: Array[String] => Unit = {
    //      val categoricalFields = List
    //      DescriptionParser.categoricalFields.map({ t =>
    //        val id = t._1
    //        val value = t._2
    //
    //      })
    //    }

    //      implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Row]
    //      labelObjectDf.map(objectRow => {
    //        val fields = objectRow.getSeq[String](0)
    //        val rawFeatures = objectRow.getSeq[Double](2)
    //        fields.zipWithIndex.foreach(tuple => {
    //          val index = tuple._2
    //          val value = tuple._1
    //          val description = DescriptionParser.allFields(index + 1)
    //          description.category match {
    //            case Category.Numeric => {
    //              rawFeatures :+ value.toDouble
    //            }
    //            case _ => {
    //            //          case Category.Categorical => labelObjectDf.col("rawCategorical_" + description.id)
    //          }
    //        })
    //        Row(fields, objectRow.get(1), rawFeatures)
    //      }).show


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

  private def numericalToRawFeatures(labelObjectDf: DataFrame) = {
    val fillNumericalCols: Seq[String] => Seq[Int] = (x) => {
      val result = new ListBuffer[Int]()
      DescriptionParser.numericFields.map({ t =>
        val id = t._1.toInt - 1
        val valueStr = x(id)
        val value = Try(valueStr.toInt).getOrElse({
          log.warn(s"Can't parse Int: $valueStr. Use 0")
          nanValue
        })
        result += value
      })
      result
    }

    val fillNumericalColsUdf = udf(fillNumericalCols)

    labelObjectDf.withColumn(rawFeaturesCol, fillNumericalColsUdf(col(objectsCol)))
  }


  private def rawCategoricalToRawFeatures(labelObjectDf1: DataFrame) = {
    var labelObjectDf = labelObjectDf1
    labelObjectDf = labelObjectDf.withColumn(rawCategoricalCol, lit(Array[Int]()))
    val fillCategoricalCols: String => (Seq[String], Seq[Int]) => Seq[Int] = column => (objects, rawFeatures) => {
      assert(objects.size == fieldsCount, objects.size)
      log.info("Objects size: " + objects.length)
      log.info("RawFeatures size: " + rawFeatures.length)
      val fieldId = column.split("_")(1).toInt - 1
      log.info("fieldsId: " + fieldId)
      val valueStr = objects(fieldId)
      val value = Try(valueStr.toInt).getOrElse({
        log.warn(s"Can't parse Int: $valueStr. Use 0")
        nanValue
      })
      val result = rawFeatures.toBuffer
      result += value
      result
    }

    labelObjectDf.columns.filter(col => col.startsWith("rawCategorical_")).foreach(column => {
      val fillCategoricalColsUdf = udf(fillCategoricalCols(column))
      labelObjectDf = labelObjectDf.withColumn(rawFeaturesCol, fillCategoricalColsUdf(col(objectsCol), col(rawFeaturesCol)))
    })
    labelObjectDf
  }

  private def objectToVector(obj: Array[String])

  = {
    obj.zipWithIndex.foreach(tuple => {
      val index = tuple._2
      val value = tuple._1
      val description = DescriptionParser.allFields(index)
      description match {
        case Category.Numeric => value.toDouble
        case Category.Categorical =>
      }
      index match {
        case `nanValue` => value.toInt
        case _ => throw new IllegalArgumentException("Unexpected index: " + index)
      }
    })
  }

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
    val trainingData = labelledVectors(nanValue)
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