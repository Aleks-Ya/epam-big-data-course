package lesson4

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._

object Main {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .appName("Iablokov Lesson 3")
      .master("local[*]")
      .getOrCreate()
    val sqlContext = ss.sqlContext
    import sqlContext.implicits._

    val vectorsPath = resourceToPath("Objects.csv")
    val vectors = ss.read.textFile(vectorsPath)
      .map(line => line.replaceAll(",", "."))
      .map(line => line.split(";"))
      .map(values => values.map(value => value.toDouble).array)
      .withColumnRenamed("value", "features")
      .withColumn("id", monotonically_increasing_id())
    vectors.show
    //    val rdd =    replaced .rdd
    //      .map(doubles => Row(doubles))
    //    rdd.take(5).foreach(println)
    //
    //    val vectors = ss.createDataFrame(rdd, makeSchema)
    //    vectors.show


    //    val vectorsPath = resourceToPath("Objects.csv")
    //    val featuresDf = ss.read
    //      .option("delimiter", ";")
    //      .option("nullValue", "NaN")
    ////      .schema(schema)
    //      .csv(vectorsPath)
    //    featuresDf.show
    //    import sqlContext.implicits._
    //        val vectors = featuresDf
    //          .map(row => row.toSeq)
    //          .map(row => row.map(value => value.toString.toDouble))
    //        vectors.show

    val labelsPath = resourceToPath("Target.csv")
    val labels = ss.read.csv(labelsPath)
      .withColumnRenamed("_c0", "label")
      .withColumn("id", monotonically_increasing_id())
    labels.show

    val labelledVectors = vectors.join(labels, "id")
    labelledVectors.show

    val estimator = new LinearRegression().setMaxIter(10)
    val model = estimator.fit(vectors)

    ss.close
  }

  private def makeSchema = {
    val intField: (String) => StructField = (name: String) => StructField(name, IntegerType, nullable = true)
    val doubleField: (String) => StructField = (name: String) => StructField(name, DoubleType, nullable = true)
    //    val fields = (for (i <- 0 to 50) yield intField("f" + i))
    //      .patch(48, Seq(doubleField("f48"), doubleField("f49")), 2)
    val fields = for (i <- 0 to 50) yield doubleField("f" + i)
    val schema = StructType(fields)
    schema.printTreeString()
    schema
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