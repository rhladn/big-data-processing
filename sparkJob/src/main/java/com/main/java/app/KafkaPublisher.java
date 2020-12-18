package com.main.java.app;

import com.main.java.avro.EmployeeProfile;
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
     * @param userCohortRDD rdd consisting of UserCohort data which is pushed to kafka key -> userId value -> EntityRelevance
     *                      EntityRelevance consists of a list of inference where each inference signifies a cohort
     * @param countersMap   The function is used to push data to kafka
     */
    private static void publishUCtoKafka(JavaPairRDD<String, EmployeeProfile> userCohortRDD, Map<String, LongAccumulator> countersMap) {
        userCohortRDD.foreach(tuple -> {
            if (PushToKafka.getInstance().push(tuple._1, getEntityRelevance(tuple._2))) {
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
        SparkAvroUtils.setAvroInputKeyValue(javaSparkContext, Schema.create(Schema.Type.STRING).toString(), EmployeeProfile.getClassSchema().toString());
        JavaPairRDD<String, EmployeeProfile> userCohortRDD = SparkAvroUtils.readAsKeyValueInputeAvro(javaSparkContext, inputFilePath)
                .mapToPair(tuple->{
                    String userId = (String) tuple._1.datum();
                    EmployeeProfile userCohort = EmployeeProfile.newBuilder((EmployeeProfile) tuple._2.datum()).build();
                    return new Tuple2<>(userId, userCohort);
                });
        publishUCtoKafka(userCohortRDD, countersMap);
        javaSparkContext.stop();
    }
}
