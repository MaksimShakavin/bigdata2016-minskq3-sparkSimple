# bigdata2016-minskq3-sparkSimple

###How to run
```
mvn clean install
```

```
 /usr/hdp/current/spark2-client/bin/spark-submit --class com.epam.bigdata2016.Main --master local[2] target/SparkApp-1.0-S
NAPSHOT-jar-with-dependencies.jar  <logs directory> <results directory> <tags dictionay> <cities dictionary>
```

e.g. 

 /usr/hdp/current/spark2-client/bin/spark-submit --class com.epam.bigdata2016.Main --master local[2] target/SparkApp-1.0-S
NAPSHOT-jar-with-dependencies.jar  hdfs://sandbox.hortonworks.com:8020/tmp/spark1 hdfs://sandbox.hortonworks.com:8020/tmp/sparkRes hdfs://sandbox.hortonworks.com:8020/tm
p/dictionaries/tags.txt hdfs://sandbox.hortonworks.com:8020/tmp/dictionaries/cities.txt
