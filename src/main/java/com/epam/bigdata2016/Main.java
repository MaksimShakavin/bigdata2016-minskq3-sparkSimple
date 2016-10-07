package com.epam.bigdata2016;

import com.epam.bigdata2016.model.*;
import com.restfb.*;
import com.restfb.types.Event;
import com.restfb.types.Location;
import com.restfb.types.Place;
import com.restfb.types.User;
import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class Main {
    private static final String FACEBOOK_TOKEN = "EAACEdEose0cBAIDJYfIFhVXZCgdk34eZCyx35kGs02qWCZC6qZB4pRYdMYPMKukTHJVpldXldtnHhS4czIko4lISCFRh535DpYt5i70L2VbNBUVDDL9qhI3lDXAkZCvvN0gbxcXSVmMiYj9DP5QevbGRyInWIvy4S1PNNZA8BiJQZDZD";
    private static final FacebookClient facebookClient = new DefaultFacebookClient(FACEBOOK_TOKEN, Version.VERSION_2_5);
    private static final SimpleDateFormat dt = new SimpleDateFormat("yyyy-mm-dd");

    public static void main(String[] args) {
        String filePath = args[0];
        String resultPath = args[1];
        String tagsFilePath = args[2];
        String citiesFilePath = args[3];

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();

        DictionaryUtils dict = new DictionaryUtils(spark,tagsFilePath,citiesFilePath);
        Map<String, String[]> tags = dict.loadTags().collectAsMap();
        Map<String, String> cities = dict.loadCities().collectAsMap();

        //Collecting all unique keyword per day per location
        JavaPairRDD<Tuple2<String, String>, String> logs =
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
                            String city = cities.getOrDefault(logLine.getCityId(),"undefined");
                            Tuple2<String, String> key = new Tuple2<>(city, logLine.getTimestamp());
                            return new Tuple2<>(key, tags.get(logLine.getTagsId()));
                        })
                        .filter(pair -> pair._2() != null)
                        .aggregateByKey(new HashSet<String>(),
                                (set, tagsArr) -> {
                                    set.addAll(Arrays.asList(tagsArr));
                                    return set;
                                },
                                (set1, set2) -> {
                                    set1.addAll(set2);
                                    return set1;
                                }
                        )
                        .flatMapValues(set -> set);

        logs.saveAsTextFile(resultPath+"/result/tagsResult");

        //Collecting all events per key:  (day,city,tag) -> eventInfo
        JavaPairRDD<DayCityTagKey, EventInfo> keyEvent =
                spark.read().textFile(tagsFilePath)
                        //Collect all unique tags
                        .javaRDD()
                        .map(line -> line.split("\\t"))
                        .map(arr -> arr[1].split(","))
                        .flatMap(arr -> Arrays.asList(arr).iterator())
                        .distinct()
                        //Fetch from fb from (tag) (day,city,tag) -> (attenders,description)
                        .flatMapToPair(tag -> {
                            //for each tag get all events
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

        //Aggregating all events to (tag,day,city) ->{ total_visitors, token_map}
        JavaPairRDD<DayCityTagKey, Tuple2<Long, List<Tuple2<String, Long>>>> resultRdd =
                // collecting (tag,day,city) -> { total_visitors, HashMap<word,amount>}
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
                            result2.setTotalAmountVisitors(result2.getTotalAmountVisitors() + result1.getTotalAmountVisitors());
                            return result2;
                        })
                        // collecting (tag,day,city) -> { total_visitors, top 10 words}
                        .mapValues(result -> {
                                    List<Tuple2<String, Long>> resultMap = result.getTokenMap()
                                            .entrySet()
                                            .stream()
                                            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                                            .map(entry -> new Tuple2<>(entry.getKey(), entry.getValue()))
                                            .limit(10).collect(Collectors.toList());
                                    return new Tuple2<>(result.getTotalAmountVisitors(), resultMap);

                                }
                        );

        resultRdd.saveAsTextFile(resultPath+"/result/eventsResult");

        //Collect all attenders
        JavaRDD<UserInfo> allAttenders = keyEvent.flatMap(pair -> {
            Connection<User> attendesConncetions = facebookClient.fetchConnection(pair._2().getId() + "/attending", User.class, Parameter.with("limit", 1000));
            return StreamSupport.stream(attendesConncetions.spliterator(), false)
                    .flatMap(Collection::stream)
                    .map(user -> new UserInfo(user.getId(), user.getName()))
                    .iterator();
        });

        //Sort all attenders by ocurances
        JavaPairRDD<UserInfo, Integer> userResultRdd =
                allAttenders
                        .mapToPair(user -> new Tuple2<>(user, 1))
                        .reduceByKey((p1, p2) -> p1 + p2)
                        .mapToPair(Tuple2::swap)
                        .sortByKey(false)
                        .mapToPair(Tuple2::swap);

        userResultRdd.saveAsTextFile(resultPath+"/result/attenders");
    }
}
