package com.epam.bigdata2016;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class DictionaryUtils {

    private SparkSession sc;

    public DictionaryUtils(SparkSession sc) {
        this.sc = sc;
    }

    public JavaPairRDD<String, String[]> loadTags() {
        return sc.read().textFile("hdfs://sandbox.hortonworks.com:8020/tmp/dictionaries/tags.txt")
                .javaRDD()
                .map(line -> line.split("\\t"))
                .mapToPair(arr -> {
                    String id = arr[0];
                    String[] tags = arr[1].split(",");
                    return new Tuple2<>(id, tags);
                });
    }

    public JavaPairRDD<String, String> loadCities() {
        return sc.read().textFile("hdfs://sandbox.hortonworks.com:8020/tmp/dictionaries/cities.txt").javaRDD()
                .map(line -> line.split("\\t"))
                .mapToPair(arr -> new Tuple2<>(arr[0], arr[1]));
    }

    ;
}
