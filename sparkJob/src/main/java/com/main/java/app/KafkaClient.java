package com.main.java.app;

import com.main.java.avro.EmployeeCategory;
import com.main.java.counters.Counters;
import com.main.java.utils.*;
import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * created by rahul.tandon
 */
public class KafkaClient {

    /**
     * @param empProfileRDD rdd consisting of employee Category data which is pushed to kafka key -> empId value -> employee details
     * @param countersMap   The function is used to push data to kafka
     */
    private void publishtoKafka(JavaPairRDD<String, EmployeeCategory> empProfileRDD, Map<String, LongAccumulator> countersMap) {

        Map<String, Object> kafkaConfig = new HashMap();
        KafkaPublisher kafkaPublisher = new KafkaPublisher(kafkaConfig, Metricregistry.getInstance(MetricRegistryName.NAME));
        empProfileRDD.foreach(tuple -> {
            if (kafkaPublisher.publish(tuple._1, tuple._2) == ExitCode.SUCCESS) {
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
        new KafkaClient().publishtoKafka(empCategoryRDD, countersMap);
        javaSparkContext.stop();
    }
}
