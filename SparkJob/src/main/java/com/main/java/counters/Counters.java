package com.main.java.counters;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.HashMap;
import java.util.Map;

public class Counters {
    /**
     * Created by rahul.tandon
     * Counters for employee profile and kafka publishing
     */

    public static String USER_PROFILE_PAIRS = "USER_PROFILE_PAIRS";
    public static String NUMBER_OF_EMPLOYEE = "NUMBER_OF_EMPLOYEE";
    public static String NUMBER_OF_PROFILE = "NUMBER_OF_PROFILE";
    public static String ONE_PROFILE = "ONE_PROFILE";
    public static String TWO_PROFILES = "TWO_PROFILES";
    public static String THREE_PROFILES = "THREE_PROFILES";
    public static String FOUR_PROFILES = "FOUR_PROFILES";
    public static String FIVE_PROFILES = "FIVE_PROFILES";
    public static String MORE_THAN_FIVE_PROFILES = "MORE_THAN_FIVE_PROFILES";
    public static String KAFKA_FAILURES = "KAFKA_FAILURES";
    public static String KAFKA_SUCCESS = "KAFKA_SUCCESS";

    public static Map<String, LongAccumulator> createUCCounters(JavaSparkContext javaSparkContext) {
        Map<String, LongAccumulator> countersMap = new HashMap<>();
        countersMap.put(Counters.ONE_PROFILE, javaSparkContext.sc().longAccumulator(Counters.ONE_PROFILE));
        countersMap.put(Counters.TWO_PROFILES, javaSparkContext.sc().longAccumulator(Counters.TWO_PROFILES));
        countersMap.put(Counters.THREE_PROFILES, javaSparkContext.sc().longAccumulator(Counters.THREE_PROFILES));
        countersMap.put(Counters.FOUR_PROFILES, javaSparkContext.sc().longAccumulator(Counters.FOUR_PROFILES));
        countersMap.put(Counters.FIVE_PROFILES, javaSparkContext.sc().longAccumulator(Counters.FIVE_PROFILES));
        countersMap.put(Counters.MORE_THAN_FIVE_PROFILES, javaSparkContext.sc().longAccumulator(Counters.MORE_THAN_FIVE_PROFILES));
        countersMap.put(Counters.USER_PROFILE_PAIRS, javaSparkContext.sc().longAccumulator(Counters.USER_PROFILE_PAIRS));
        countersMap.put(Counters.NUMBER_OF_PROFILE, javaSparkContext.sc().longAccumulator(Counters.NUMBER_OF_PROFILE));
        countersMap.put(Counters.NUMBER_OF_EMPLOYEE, javaSparkContext.sc().longAccumulator(Counters.NUMBER_OF_EMPLOYEE));
        return countersMap;
    }

    public static Map<String, LongAccumulator> createKafkaCounters(JavaSparkContext javaSparkContext) {
        Map<String, LongAccumulator> countersMap = new HashMap<>();
        countersMap.put(Counters.KAFKA_FAILURES, javaSparkContext.sc().longAccumulator(Counters.KAFKA_FAILURES));
        countersMap.put(Counters.KAFKA_SUCCESS, javaSparkContext.sc().longAccumulator(Counters.KAFKA_SUCCESS));
        return countersMap;
    }
}