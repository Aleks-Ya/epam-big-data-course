BigData, Spark, Lesson 1
Home work
Iablokov Aleksei

Requirements:
1. Scala (tested on 2.12.1)
2. [for Windows] HADOOP_HOME environment points to directory contains bin\winutils.exe
3. SBT (tested on 0.13.13) with access to Internet for downloading

Run:
sbt test


There are 2 tests: SmallDataTest and RealDataTest

SmallDataTest tests the algorithm on small data set and only prints Top5 IP to console

RealDataTest process real data set (access.log, ~3Mb) including prints Top5 IP and creates CSV file contains all IPs
Input file location is PROJECT_DIR\src\test\resources\access.log
Output CSV file location is PROJECT_DIR\target\output.csv\part-XXXX