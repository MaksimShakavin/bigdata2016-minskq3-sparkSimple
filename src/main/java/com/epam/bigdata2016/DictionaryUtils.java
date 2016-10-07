package com.epam.bigdata2016;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class DictionaryUtils {

    private SparkSession sc;
    private String tagsFile;
    private String citiesFile;

    public DictionaryUtils(SparkSession sc, String tagsFile, String citiesFile) {
        this.sc = sc;
        this.tagsFile = tagsFile;
        this.citiesFile = citiesFile;
    }

    public JavaPairRDD<String, String[]> loadTags() {
        return sc.read().textFile(tagsFile)
                .javaRDD()
                .map(line -> line.split("\\t"))
                .mapToPair(arr -> {
                    String id = arr[0];
                    String[] tags = arr[1].split(",");
                    return new Tuple2<>(id, tags);
                });
    }

    public JavaPairRDD<String, String> loadCities() {
        return sc.read().textFile(citiesFile).javaRDD()
                .map(line -> line.split("\\t"))
                .mapToPair(arr -> new Tuple2<>(arr[0], arr[1]));
    }

    ;
}
