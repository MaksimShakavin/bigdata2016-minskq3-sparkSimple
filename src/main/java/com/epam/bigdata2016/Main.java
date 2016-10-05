package com.epam.bigdata2016;

import com.epam.bigdata2016.model.LogLine;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class Main {
    public static void main(String[] args) {
        String filePath = args[0];
        String resultPath = args[1];

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();

        DictionaryUtils dict = new DictionaryUtils(spark);
        Map<String, String[]> tags = dict.loadTags().collectAsMap();
        Map<String, String> cities = dict.loadCities().collectAsMap();

        JavaPairRDD<Tuple2<Long, String>, HashSet<String>> logs =
                spark.read().textFile(filePath)
                        .javaRDD()
                        .map(line -> line.split("\\t"))
                        .map(arr -> {
                            String tagid = arr[20];
                            String timestamp = arr[1].substring(0, 8);
                            Long cityId = Long.valueOf(arr[6]);
                            return new LogLine(cityId, timestamp, tagid);
                        })
                        .mapToPair(logLine -> {
                            Tuple2<Long, String> key = new Tuple2<>(logLine.getCityId(), logLine.getTimestamp());
                            return new Tuple2<>(key, tags.get(logLine.getTagsId()));
                        })
                        .aggregateByKey(new HashSet<String>(),
                                (set, tagsArr) -> {
                                    set.addAll(Arrays.asList(tagsArr));
                                    return set;
                                },
                                (set1, set2) -> {
                                    set1.addAll(set2);
                                    return set1;
                                }
                        );
        logs.foreach(line -> System.out.println(line._1()._1() + " " +  line._1()._2() + " " + line._2()));

    }
}
