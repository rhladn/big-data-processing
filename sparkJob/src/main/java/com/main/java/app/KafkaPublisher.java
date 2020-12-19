package com.main.java.app;

import com.main.java.avro.EmployeeCategory;
import com.main.java.counters.Counters;
import com.main.java.utils.PushToKafka;
import com.main.java.utils.SparkAvroUtils;
import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.Map;

public class KafkaPublisher {

    /**
     * @param empProfileRDD rdd consisting of employee Category data which is pushed to kafka key -> empId value -> employee details
     * @param countersMap   The function is used to push data to kafka
     */
    private static void publishtoKafka(JavaPairRDD<String, EmployeeCategory> empProfileRDD, Map<String, LongAccumulator> countersMap) {

        empProfileRDD.foreach(tuple -> {
            if (PushToKafka.push(tuple._1, tuple._2)) { // function to push to Kafka with empId as key and category as value
                countersMap.get(Counters.KAFKA_SUCCESS).add(1L);
            } else {
                countersMap.get(Counters.KAFKA_FAILURES).add(1L);
            }
        });
    }

    public static void main(String[] args) {

        String inputFilePath = args[1];
        SparkConf sparkConf = new SparkConf();
        JavaSparkContext javaSparkContext = new JavaSparkContext(new SparkContext(sparkConf));
        javaSparkContext.hadoopConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,"
                + "org.apache.hadoop.io.compress.DefaultCodec,"
                + "org.apache.hadoop.io.compress.SnappyCodec,"
                + "org.apache.hadoop.io.compress.BZip2Codec");
        Map<String, LongAccumulator> countersMap = Counters.createKafkaCounters(javaSparkContext);
        SparkAvroUtils.setAvroInputKeyValue(javaSparkContext, Schema.create(Schema.Type.STRING).toString(), EmployeeCategory.getClassSchema().toString());
        JavaPairRDD<String, EmployeeCategory> empCategoryRDD = SparkAvroUtils.readAsKeyValueInputeAvro(javaSparkContext, inputFilePath)
                .mapToPair(tuple->{
                    String empId = (String) tuple._1.datum();
                    EmployeeCategory empCategory = EmployeeCategory.newBuilder((EmployeeCategory) tuple._2.datum()).build();
                    return new Tuple2<>(empId, empCategory);
                });
        publishtoKafka(empCategoryRDD, countersMap);
        javaSparkContext.stop();
    }
}
