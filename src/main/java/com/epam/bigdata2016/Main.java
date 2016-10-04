package com.epam.bigdata2016;

import com.epam.bigdata2016.model.LogLine;
import org.apache.spark.api.java.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;


public class Main {
    public static void main(String[] args) {
        String filePath = args[0];
        String resultPath = args[1];

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();

        JavaRDD<LogLine> logs = spark.read().textFile(filePath)
                .javaRDD()
                .map(line -> line.split("\\t")).map(arr -> {
                    String tagid = arr[20];
                    String timestamp = arr[1].substring(0,8);
                    Long cityId = Long.valueOf(arr[6]);
                    return new LogLine(cityId, timestamp, tagid);
                });

        Encoder<LogLine> logEnc = Encoders.bean(LogLine.class);

        Dataset<LogLine> df = spark.createDataset(logs.rdd(), logEnc);

        df.limit(10).show();


    }
}
