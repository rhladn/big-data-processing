package com.main.java.counters;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rahul.tandon
 * Counters for employee category and kafka publishing
 */
public class Counters {

    public static String EMPLOYEE_CATEGORY_PAIRS = "EMPLOYEE_CATEGORY_PAIRS";
    public static String NUMBER_OF_EMPLOYEE = "NUMBER_OF_EMPLOYEE";
    public static String NUMBER_OF_CATEGORY = "NUMBER_OF_CATEGORY";
    public static String ONE_CATEGORY = "ONE_CATEGORY";
    public static String TWO_CATEGORIES = "TWO_CATEGORIES";
    public static String THREE_CATEGORIES = "THREE_CATEGORIES";
    public static String FOUR_CATEGORIES = "FOUR_CATEGORIES";
    public static String FIVE_CATEGORIES = "FIVE_CATEGORIES";
    public static String MORE_THAN_FIVE_CATEGORIES = "MORE_THAN_FIVE_CATEGORIES";
    public static String KAFKA_FAILURES = "KAFKA_FAILURES";
    public static String KAFKA_SUCCESS = "KAFKA_SUCCESS";

    public static Map<String, LongAccumulator> createUCCounters(JavaSparkContext javaSparkContext) {
        Map<String, LongAccumulator> countersMap = new HashMap<>();
        countersMap.put(Counters.ONE_CATEGORY, javaSparkContext.sc().longAccumulator(Counters.ONE_CATEGORY));
        countersMap.put(Counters.TWO_CATEGORIES, javaSparkContext.sc().longAccumulator(Counters.TWO_CATEGORIES));
        countersMap.put(Counters.THREE_CATEGORIES, javaSparkContext.sc().longAccumulator(Counters.THREE_CATEGORIES));
        countersMap.put(Counters.FOUR_CATEGORIES, javaSparkContext.sc().longAccumulator(Counters.FOUR_CATEGORIES));
        countersMap.put(Counters.FIVE_CATEGORIES, javaSparkContext.sc().longAccumulator(Counters.FIVE_CATEGORIES));
        countersMap.put(Counters.MORE_THAN_FIVE_CATEGORIES, javaSparkContext.sc().longAccumulator(Counters.MORE_THAN_FIVE_CATEGORIES));
        countersMap.put(Counters.EMPLOYEE_CATEGORY_PAIRS, javaSparkContext.sc().longAccumulator(Counters.EMPLOYEE_CATEGORY_PAIRS));
        countersMap.put(Counters.NUMBER_OF_CATEGORY, javaSparkContext.sc().longAccumulator(Counters.NUMBER_OF_CATEGORY));
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