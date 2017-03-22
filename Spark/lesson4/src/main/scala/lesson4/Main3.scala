package lesson4

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types._
import org.apache.spark.util.SizeEstimator
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.util.Try

object Main3 {
  private val log = LoggerFactory.getLogger(getClass)
  private val labelCol = "label"
  private val objectsCol = "objects"
  //  private val rawFeaturesCol = "rawFeatures"
  private val featuresCol = "features"

  private val fieldsCount = 50

  private val categoricalCol = "categorical"
  private val categoricalVectorCol = "categoricalVector"
  private val categoricalPrefix = "categorical_"
  private val rawCategoricalPrefix = "rawCategorical_"

  def main(args: Array[String]): Unit = {
    val ss = SparkHelper.ss


    val descriptions = new DescriptionParser(FileHelper.readDescriptions)

    val objectsRdd: RDD[(Long, Array[String])] = FileHelper.readObjects(ss)
      .zipWithIndex().map(_.swap)

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


    val featureRdd: RDD[(Long, Array[Double])] = objectsRdd.map(t => {
      val objId = t._1
      val objValues = t._2
      val features = objValues.zipWithIndex
        .filter(t => descriptions.numericFields.contains(t._2))
        .map(t => parseDouble(t._1))
      (objId, features)
    })






    val labelsRdd: RDD[Int] = FileHelper.readLabels(ss)
    val labelObjectRdd = objectsRdd.zip(labelsRdd).map(tuple => Row(tuple._1, tuple._2))
    var df: DataFrame = rddToDf(ss, labelObjectRdd)
      .withColumn(featuresCol, lit(null))
      .withColumn(categoricalCol, lit(null))
      .withColumn(categoricalVectorCol, lit(null))
    df.show


    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Row]
    df = df.map(row => {
      val objectsColInd = row.fieldIndex(objectsCol)
      val featuresColInd = row.fieldIndex(featuresCol)
      val features = UdfFunctions.numericalToRawFeatures(descriptions)(row.getSeq[String](objectsColInd))

      val rowSeq = row.toSeq.toBuffer
      rowSeq(featuresColInd) = features
      Row(rowSeq)
    }).toDF("objects", "label", "features", "categorical", "categoricalVector")
    df.show

    UdfFunctions.numericalToRawFeatures(descriptions)
    df = numericalToRawFeatures(descriptions, df)
    //    labelObjectDf.show

    df = categoricalObjectToCategorical(df)
    //    labelObjectDf.cache
    //    labelObjectDf.show

    df = transformCategoricalToRawCategorical(df)
    //    labelObjectDf.cache
    //    labelObjectDf.show

    df = appendRawCategoricalToRawFeatures(df)
    //    labelObjectDf.cache
    //    labelObjectDf.show

    df = rawFeaturesToLabelledPoint(df)
    //    labelObjectDf.cache
    //    labelObjectDf.show

    log.info("Size before columns drop: " + SizeEstimator.estimate(df))
    df = dropUnusedColumns(df)
    log.info("Start to cache")

    df.cache
    df.explain(extended = true)
    log.info("Size after caching: " + SizeEstimator.estimate(df))
    val featureCount = df.select(featuresCol)
      .first
      .get(0)
      .asInstanceOf[org.apache.spark.ml.linalg.Vector]
      .size
    log.info("Feature count: " + featureCount)
    //    labelObjectDf.show

    val (trainingData: Dataset[Row], testData: Dataset[Row]) = splitInputData(df)
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
    labelObjectDf = labelObjectDf.drop(col(featuresCol))
    labelObjectDf
  }

  private def addCategoricalColumns(descriptionParser: DescriptionParser, labelObjectDf1: DataFrame) = {
    log.info("Enter addCategoricalColumns")
    var labelObjectDf = labelObjectDf1
    descriptionParser.categoricalFields.foreach { t =>
      val id = t._1.toInt - 1
      val colName = categoricalPrefix + id
      labelObjectDf = labelObjectDf.withColumn(colName, lit(-1))
    }
    //    labelObjectDf.show
    labelObjectDf
  }

  private def numericalToRawFeatures(descriptionParser: DescriptionParser, labelObjectDf: DataFrame) = {
    log.info("Enter numericalToRawFeatures")
    val fillNumericalColsUdf = udf(UdfFunctions.numericalToRawFeatures(descriptionParser))
    labelObjectDf.withColumn(featuresCol, fillNumericalColsUdf(col(objectsCol)))
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

    labelObjectDf.columns.filter(col => col.startsWith(rawCategoricalPrefix)).foreach(rawCategoricalColumn => {
      log.debug("Process rawCategoricalColumn: " + rawCategoricalColumn)
      //      val fillCategoricalColsUdf = udf(UdfFunctions.appendRawCategoricalToRawFeatures)
      //      labelObjectDf.select(rawFeaturesCol).foreach(row => println(row))
      //      labelObjectDf = labelObjectDf.withColumn(rawFeaturesCol, fillCategoricalColsUdf(col(rawCategoricalColumn), col(rawFeaturesCol)))
      val sql = SparkHelper.ss.sqlContext
      implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Row]
      log.debug("labelObjectDf size: " + labelObjectDf.count())
      labelObjectDf = labelObjectDf.map(row => {
        val rawFeaturesIndex = row.fieldIndex(featuresCol)
        val vectorCat = row.getAs[org.apache.spark.ml.linalg.Vector](rawCategoricalColumn)
        //        val vector = row.getAs[org.apache.spark.ml.linalg.Vector](rawFeaturesCol)
        val rawFeatures = row.getSeq[Double](rawFeaturesIndex)
        val res = ListBuffer[Double]()
        res ++= rawFeatures
        res ++= vectorCat.toDense.toArray
        val newRowArray = row.toSeq.toBuffer
        newRowArray(rawFeaturesIndex) = res.toList
        //        RowFactory.create(newRowArray)
        Row(newRowArray)
      })
    })
    labelObjectDf
  }

  private def rawFeaturesToLabelledPoint(labelObjectDf1: DataFrame) = {
    log.info("Enter rawFeaturesToLabelledPoint")
    var labelObjectDf = labelObjectDf1
    val udfObj = udf(UdfFunctions.rawFeaturesToVector)
    labelObjectDf = labelObjectDf.withColumn(featuresCol, udfObj(col(featuresCol)))
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


  private def printSummary(model: LinearRegressionModel) = {
    log.info("Enter printSummary")
    val trainingSummary = model.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
    trainingSummary.residuals.show()
  }

}