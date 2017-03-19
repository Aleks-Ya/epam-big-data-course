package lesson4

import org.apache.spark.ml.linalg.{VectorUDTPublic, Vectors}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.slf4j.LoggerFactory

object MainDataFrame {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .appName("Iablokov Lesson 3")
      .master("local[*]")
      .getOrCreate()

    val vectorsPath = resourceToPath("Objects.csv")
    val rdd = ss.sparkContext.textFile(vectorsPath)
      .map(line => line.replaceAll(",", "."))
      .map(line => line.split(";").map(value => value.toDouble).array)
      .map(array => Vectors.dense(array))
      .map(vector => Row(vector))
    assert(rdd.count() == 15223)

    val schema = StructType(
      StructField("features", VectorUDTPublic, nullable = false) :: Nil
    )

    val vectorsDf = ss.createDataFrame(rdd, schema)
      .withColumn("id", monotonically_increasing_id())

    val labelsPath = resourceToPath("Target.csv")
    val labels = ss.read.csv(labelsPath)
      .withColumnRenamed("_c0", "label")
      .withColumn("id", monotonically_increasing_id())
    labels.show

    val labelledVectors = vectorsDf.join(labels, "id")
    labelledVectors.show

    val estimator = new LinearRegression().setMaxIter(10).setLabelCol("label").setFeaturesCol("features")
    val model = estimator.fit(labelledVectors)

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