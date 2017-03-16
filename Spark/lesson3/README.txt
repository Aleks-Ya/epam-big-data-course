BigData, Spark, Lesson 3
Home work
Iablokov Aleksei

Requirements:
1. Scala (tested on 2.12.1)
2. SBT (tested on 0.13.13) with access to Internet for downloading

Run:
1. Kafka
    1. Run Zookeeper on port 2181
    2. Run Kafka server on port 9092
2. Setup Hive with metastore
3. Build iablokov_spark_3.jar: sbt assembly
4. Install "libcap":yum install libcap-devel
5. Submit Spark application: spark-submit iablokov_spark_3.jar

