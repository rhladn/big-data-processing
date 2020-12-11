package com.main.java.counters;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.HashMap;
import java.util.Map;

public class Counters {
    /**
     * Created by rahul.tandon
     * Counters for user cohort and kafka publishing
     */

    public static String USER_COHORT_PAIRS = "USER_COHORT_PAIRS";
    public static String NUMBER_OF_USERS = "NUMBER_OF_USERS";
    public static String NUMBER_OF_COHORTS = "NUMBER_OF_COHORTS";
    public static String ONE_COHORT = "ONE_COHORT";
        public static String TWO_COHORTS = "TWO_COHORTS";
        public static String THREE_COHORTS = "THREE_COHORTS";
        public static String FOUR_COHORTS = "FOUR_COHORTS";
        public static String FIVE_COHORTS = "FIVE_COHORTS";
        public static String MORE_THAN_FIVE_COHORTS = "MORE_THAN_FIVE_COHORTS";
        public static String KAFKA_FAILURES = "KAFKA_FAILURES";
        public static String KAFKA_SUCCESS = "KAFKA_SUCCESS";

        public static Map<String, LongAccumulator> createUCCounters(JavaSparkContext javaSparkContext) {
            Map<String, LongAccumulator> countersMap = new HashMap<>();
            countersMap.put(Counters.ONE_COHORT, javaSparkContext.sc().longAccumulator(Counters.ONE_COHORT));
            countersMap.put(Counters.TWO_COHORTS, javaSparkContext.sc().longAccumulator(Counters.TWO_COHORTS));
            countersMap.put(Counters.THREE_COHORTS, javaSparkContext.sc().longAccumulator(Counters.THREE_COHORTS));
            countersMap.put(Counters.FOUR_COHORTS, javaSparkContext.sc().longAccumulator(Counters.FOUR_COHORTS));
            countersMap.put(Counters.FIVE_COHORTS, javaSparkContext.sc().longAccumulator(Counters.FIVE_COHORTS));
            countersMap.put(Counters.MORE_THAN_FIVE_COHORTS, javaSparkContext.sc().longAccumulator(Counters.MORE_THAN_FIVE_COHORTS));
            countersMap.put(Counters.USER_COHORT_PAIRS, javaSparkContext.sc().longAccumulator(Counters.USER_COHORT_PAIRS));
            countersMap.put(Counters.NUMBER_OF_COHORTS, javaSparkContext.sc().longAccumulator(Counters.NUMBER_OF_COHORTS));
            countersMap.put(Counters.NUMBER_OF_USERS, javaSparkContext.sc().longAccumulator(Counters.NUMBER_OF_USERS));
            return countersMap;
        }

        public static Map<String, LongAccumulator> createKafkaCounters(JavaSparkContext javaSparkContext) {
            Map<String, LongAccumulator> countersMap = new HashMap<>();
            countersMap.put(Counters.KAFKA_FAILURES, javaSparkContext.sc().longAccumulator(Counters.KAFKA_FAILURES));
            countersMap.put(Counters.KAFKA_SUCCESS, javaSparkContext.sc().longAccumulator(Counters.KAFKA_SUCCESS));
            return countersMap;
        }
    }
