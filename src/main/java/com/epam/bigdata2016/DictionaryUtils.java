package com.epam.bigdata2016;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class DictionaryUtils {

    private JavaSparkContext sc;

    public JavaPairRDD<String, String[]> loadTags() {
        return sc.textFile("hdfs://")
                .map(line -> line.split("\\t"))
                .mapToPair(arr -> {
                    String id = arr[0];
                    String[] tags = arr[1].split(",");
                    return new Tuple2<>(id,tags);
                });
    }

    public JavaPairRDD<String, String> loadCities() {
        return sc.textFile("hdfs://")
                .map(line -> line.split("\\t"))
                .mapToPair(arr -> new Tuple2<>(arr[0], arr[1]));
    }

    ;
}
