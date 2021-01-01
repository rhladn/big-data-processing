package com.main.java.utils;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by rahul.tandon
 * Used to read and write Avro to HDFS as well as configurations for the same
 */
public class SparkAvroUtils {

    public static String AVRO_SCHEMA_OUTPUT_KEY = "avro.schema.output.key";
    public static String AVRO_SCHEMA_INPUT_KEY = "avro.schema.input.key";
    public static String AVRO_SCHEMA_OUTPUT_VALUE = "avro.schema.output.value";
    public static String AVRO_SCHEMA_INPUT_VALUE = "avro.schema.input.value";

    public static void writeAsKeyValueOutputAvro(JavaSparkContext javaSparkContext, JavaPairRDD<AvroKey, AvroValue> processedAvroRecords,
                                                 String outputFilePath) {

        processedAvroRecords.saveAsNewAPIHadoopFile(outputFilePath, AvroKey.class, AvroValue.class,
                AvroKeyValueOutputFormat.class, javaSparkContext.hadoopConfiguration());
    }

    public static JavaPairRDD<AvroKey, AvroValue> readAsKeyInputAvro(JavaSparkContext javaSparkContext, String inputFilePath) {

        return javaSparkContext.newAPIHadoopFile(inputFilePath, AvroKeyInputFormat.class,
                AvroKey.class, AvroValue.class, javaSparkContext.hadoopConfiguration());
    }

    public static JavaPairRDD<AvroKey, AvroValue> readAsKeyValueInputeAvro(JavaSparkContext javaSparkContext, String inputFilePath) {

        return javaSparkContext.newAPIHadoopFile(inputFilePath, AvroKeyValueInputFormat.class,
                AvroKey.class, AvroValue.class, javaSparkContext.hadoopConfiguration());
    }

    public static void setAvroOutputKeyValue(JavaSparkContext javaSparkContext, String outputKey, String outputValue) {

        javaSparkContext.hadoopConfiguration().set(AVRO_SCHEMA_OUTPUT_KEY, outputKey);
        javaSparkContext.hadoopConfiguration().set(AVRO_SCHEMA_OUTPUT_VALUE, outputValue);
    }

    public static void setAvroInputKeyValue(JavaSparkContext javaSparkContext, String inputKey, String inputValue) {

        javaSparkContext.hadoopConfiguration().set(AVRO_SCHEMA_INPUT_KEY, inputKey);
        javaSparkContext.hadoopConfiguration().set(AVRO_SCHEMA_INPUT_VALUE, inputValue);
    }
}
