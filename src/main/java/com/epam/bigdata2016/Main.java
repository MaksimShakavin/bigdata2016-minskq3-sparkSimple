package com.epam.bigdata2016;

import com.epam.bigdata2016.model.DayCityTagKey;
import com.epam.bigdata2016.model.EventInfo;
import com.epam.bigdata2016.model.LogLine;
import com.epam.bigdata2016.model.VisitorsTokenResult;
import com.restfb.*;
import com.restfb.types.Event;
import com.restfb.types.Location;
import com.restfb.types.Place;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class Main {
    private static final String FACEBOOK_TOKEN = "EAAbZB8vQ692kBAKKbb9buNhrTUEg69kXZBd8ZCzZAlU9k3f3y80ZAzFmDzci1UVIEGoZBCViXScfz5ccS3VGdb3XlEZClE8lldXMSc1IjIxQjEbQVTxdnZBdbkrrtRMdYy1ZAagQsx2RDZC37M5wiEN7Uz2lIxOl5uNwbCIqe3ytvbcQZDZD";
    private static final FacebookClient facebookClient = new DefaultFacebookClient(FACEBOOK_TOKEN, Version.VERSION_2_5);
    private static final SimpleDateFormat dt = new SimpleDateFormat("yyyy-mm-dd");

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
        LongAccumulator fb = spark.sparkContext().longAccumulator("processed");


//        JavaPairRDD<Tuple2<Long, String>, HashSet<String>> logs =
//                spark.read().textFile(filePath)
//                        .javaRDD()
//                        .map(line -> line.split("\\t"))
//                        .map(arr -> {
//                            String tagid = arr[20];
//                            String timestamp = arr[1].substring(0, 8);
//                            Long cityId = Long.valueOf(arr[6]);
//                            return new LogLine(cityId, timestamp, tagid);
//                        })
//                        .mapToPair(logLine -> {
//                            Tuple2<Long, String> key = new Tuple2<>(logLine.getCityId(), logLine.getTimestamp());
//                            return new Tuple2<>(key, tags.get(logLine.getTagsId()));
//                        })
//                        .filter(pair -> pair._2() != null)
//                        .aggregateByKey(new HashSet<String>(),
//                                (set, tagsArr) -> {
//                                    set.addAll(Arrays.asList(tagsArr));
//                                    return set;
//                                },
//                                (set1, set2) -> {
//                                    set1.addAll(set2);
//                                    return set1;
//                                }
//                        );

        JavaPairRDD<DayCityTagKey, EventInfo> keyEvent =
                spark.read().textFile("hdfs://sandbox.hortonworks.com:8020/tmp/dictionaries/tags.txt")
                        .javaRDD()
                        .map(line -> line.split("\\t"))
                        .map(arr -> arr[1].split(","))
                        .flatMap(arr -> Arrays.asList(arr).iterator())
                        .distinct()
                        .flatMapToPair(tag -> {
                            Connection<Event> eventConnections = facebookClient.fetchConnection("search", Event.class,
                                    Parameter.with("q", tag),
                                    Parameter.with("type", "event"),
                                    Parameter.with("fields", "id,attending_count,place,name,description,start_time"));

                            return StreamSupport.stream(eventConnections.spliterator(), false)
                                    .flatMap(Collection::stream)
                                    .filter(event -> event != null)
                                    .map(event -> {
                                        Optional<Event> eventOpt = Optional.of(event);
                                        String city = eventOpt
                                                .map(Event::getPlace)
                                                .map(Place::getLocation)
                                                .map(Location::getCity)
                                                .orElse("unknown");
                                        String date = eventOpt
                                                .map(ev -> dt.format(ev.getStartTime()))
                                                .orElse("2000-01-01");
                                        String description = eventOpt
                                                .map(Event::getDescription)
                                                .orElse("");
                                        return new Tuple2<>(
                                                new DayCityTagKey(date, city, tag),
                                                new EventInfo(event.getId(), event.getName(), description, event.getAttendingCount())
                                        );
                                    })
                                    .iterator();
                        });


        JavaPairRDD<DayCityTagKey,List<Tuple2<String,Long>>> resultRdd =
                keyEvent.aggregateByKey(new VisitorsTokenResult(),
                        (result, eventInfo) -> {
                            result.setTotalAmountVisitors(result.getTotalAmountVisitors() + eventInfo.getAttendance());
                            String descr = eventInfo.getDescription();
                            Arrays.stream(descr.split("\\s+"))
                                    .forEach(word -> {
                                        long numb = Optional.ofNullable(result.getTokenMap().get(word))
                                                .map(prev -> prev + 1)
                                                .orElse(1L);
                                        result.getTokenMap().put(word, numb);
                                    });
                            result.setTotalAmountVisitors(result.getTotalAmountVisitors() + 1);
                            return result;
                        },
                        (result1, result2) -> {
                            result1.getTokenMap()
                                    .forEach((k, v) -> result2.getTokenMap()
                                            .merge(k, v, (oldVal, newVal) -> oldVal + newVal));
                            return result2;
                        })
                        .mapValues(result -> result.getTokenMap()
                                .entrySet()
                                .stream()
                                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                                .map(entry -> new Tuple2<>(entry.getKey(), entry.getValue()))
                                .limit(10).collect(Collectors.toList())
                        );


        resultRdd.foreach(pair ->{fb.add(1L); System.out.println(pair._1()+":" + pair._2());});

    }
}
