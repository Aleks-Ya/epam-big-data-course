BigData, Spark, Lesson 3
Home work
Iablokov Aleksei

Requirements:
1. Scala (tested on 2.12.1)
2. [for Windows] HADOOP_HOME environment points to directory contains bin\winutils.exe
3. SBT (tested on 0.13.13) with access to Internet for downloading

1. Run once with Admin privileges to turn network cards to promiscuous mode
2. MySQL for Hive metastore

Run on Spark local:
sbt test

Run on HDP:
1. Download data files
	1.1. airports.csv from http://stat-computing.org/dataexpo/2009/airports.csv
	1.2. carriers.csv from http://stat-computing.org/dataexpo/2009/carriers.csv
	1.3. 2007.csv (upack) from http://stat-computing.org/dataexpo/2009/2007.csv.bz2
2. Put the data files into HDFS in directory /tmp/iablokov/spark/lesson2
3. Build iablokov_spark_2.jar: sbt assembly
4. copy target\scala-2.11\iablokov_spark_2.jar to HDP
5. Submit Spark application: spark-submit --class lesson2.Main iablokov_spark_2.jar hdfs://sandbox.hortonworks.com:8020/tmp/iablokov/spark/lesson2 
