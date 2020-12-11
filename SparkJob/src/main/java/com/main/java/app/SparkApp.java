package com.main.java.app;

import com.main.java.avro.CohortInfo;
import com.main.java.avro.Employee;
import com.main.java.avro.EmployeeProfile;
import com.main.java.avro.User;
import com.main.java.avro.UserCohort;
import com.main.java.constants.Constants;
import com.main.java.counters.Counters;
import com.main.java.utils.ApiUtils;
import com.main.java.utils.SparkAvroUtils;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * created by rahul.tandon
 * fetch the data from hdfs and create key:employeeId value:list of
 */
public class SparkApp implements Serializable {

    private JavaSparkContext javaSparkContext;
    private List<String> cohortId;
    private String outputDir;
    private static Map<String, LongAccumulator> countersMap;

    /**
     * The complete flow of Spark Job
     */
    private void process() {
        JavaPairRDD<String, String>[] userCohortRDDArray = new JavaPairRDD[cohortId.size()];
        for (int i = 0; i < cohortId.size(); i++) {
            try {
                userCohortRDDArray[i] = getUserToCohortRDD(cohortId.get(i), ApiUtils.get(Constants.url + cohortId.get(i) + "/ACCOUNTID", Constants.xClientIdKey, Constants.xClientIdValue));
            } catch (Exception e) {
                throw new RuntimeException("exception while processing cohort id " + cohortId.get(i));
            }
        }
        JavaRDD<EmployeeProfile> userCohortRDD = convert(javaSparkContext, countersMap, userCohortRDDArray);
        writeRDDtoAvro(javaSparkContext, userCohortRDD, outputDir);
    }

    /**
     * @param userCohortRDD    rdd consisting of userCohort data which is written to HDFS as key userId and value userCohort
     * @param outputDir        this is the output directory where the data is pushed which is present in config bcuket of airflow
     * @param javaSparkContext This function writes rdd to hdfs path
     */
    private void writeRDDtoAvro(JavaSparkContext javaSparkContext, JavaRDD<EmployeeProfile> userCohortRDD, String outputDir) {
        SparkAvroUtils.setAvroOutputKeyValue(javaSparkContext, Schema.create(Schema.Type.STRING).toString(), EmployeeProfile.getClassSchema().toString());
        JavaPairRDD<AvroKey, AvroValue> userCohortAvroPairRDD = userCohortRDD.mapToPair(userCohort -> new Tuple2<>(
                new AvroKey<>(userCohort.getUserId()), new AvroValue<>(userCohort)));
        SparkAvroUtils.writeAsKeyValueOutputAvro(javaSparkContext, userCohortAvroPairRDD, outputDir);
    }

    /**
     * @param cohortId      this is the cohort Id which is taken from the config bucket reco.cohort.prod
     * @param inputFilePath this is the file from where the user data is read for a cohort Id
     *                      This function gets the rdd from the inputFilepath
     */
    private JavaPairRDD<String, String> getUserToCohortRDD(String cohortId, String inputFilePath) {
        SparkAvroUtils.setAvroInputKeyValue(javaSparkContext, Employee.getClassSchema().toString(), Schema.create(Schema.Type.STRING).toString());
        JavaPairRDD<AvroKey, AvroValue> userCohortAvroRDD = SparkAvroUtils.readAsKeyInputAvro(javaSparkContext, inputFilePath);
        return userCohortAvroRDD.mapToPair(tuple -> {
            Employee user = (Employee) tuple._1.datum();
            return new Tuple2<>(user.getUserid(), cohortId);
        });
    }

    private void initSparkConfigs() {
        SparkConf sparkConf = new SparkConf();
        javaSparkContext = new JavaSparkContext(new SparkContext(sparkConf));
        javaSparkContext.hadoopConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,"
                + "org.apache.hadoop.io.compress.DefaultCodec,"
                + "org.apache.hadoop.io.compress.SnappyCodec,"
                + "org.apache.hadoop.io.compress.BZip2Codec");
    }

    /**
     * @param cohortDict this is the cohorts which are fetched from the config bucket reco.cohort.prod
     *                   This function gets the cohorts from config bucket
     */
    private void getListOfCohorts(Map<String, String> cohortDict) {
        setCounters(javaSparkContext);
        cohortId = new ArrayList<>();
        for (Map.Entry<String, String> entry : cohortDict.entrySet())
            cohortId.add(entry.getKey());
        countersMap.get(Counters.NUMBER_OF_COHORTS).add(cohortDict.size());
    }

    /**
     * @param bucketName cohortId#cohortName seperated by _ delimitor
     * @return Map consisting of cohortId as key and cohortName as value
     * */
    private Map<String, String> getMapFromBucket(String bucketName){
        Map<String, String> bucket = new HashMap<>();
        String[] buckets = bucketName.split("_");
        for(int i=0; i<buckets.length; i++)
            bucket.put(buckets[i].split(Constants.HASH_DELIMITER)[0], buckets[i].split(Constants.HASH_DELIMITER)[1]);
        return bucket;
    }

    /**
     * @param bucketName reco.cohort.prod config bucket which contains the cohortIds
     * @param outputDir  sets the outputDir where the data is published to HDFS for hawk consumption
     */
    private void loadConfig(String bucketName, String outputDir) {
        Map<String, String> cohortDict  = getMapFromBucket(bucketName);
        this.outputDir = outputDir;
        getListOfCohorts(cohortDict);
    }

    private void stopAllContext() {
        javaSparkContext.stop();
    }

    /**
     * @param javaSparkContext Creates conters for the user Cohort job
     */
    private void setCounters(JavaSparkContext javaSparkContext) {
        countersMap = Counters.createUCCounters(javaSparkContext);
    }

    /**
     * @param javaSparkContext
     * @param countersMap
     * @param userCohortRDDArray this is an array of rdd in which each element is an rdd of userId and cohortId
     * @return userCohortRDD this is the UserCohort rdd which is returned consisting of user cohort data
     * This function converts array of rdd which contains the cohort, userId pairs to user to cohort rdd
     */
    private JavaRDD<EmployeeProfile> convert(JavaSparkContext javaSparkContext, Map<String, LongAccumulator> countersMap, JavaPairRDD<String, String>[] userCohortRDDArray) {
        JavaPairRDD<String, String> unionRDD = javaSparkContext.union(userCohortRDDArray);
        JavaRDD<EmployeeProfile> userCohortRDD = unionRDD
                .mapToPair(usersToCohort -> {
                    countersMap.get(Counters.USER_COHORT_PAIRS).add(1L);
                    return new Tuple2<>(usersToCohort._1, usersToCohort._2);
                })
                .groupByKey()
                .map(pair -> {
                    List<String> cohortList = new ArrayList<>();
                    List<CohortInfo> cohortInfoList = new ArrayList<>();
                    cohortList.addAll((Collection<? extends String>) pair._2);
                    EmployeeProfile userCohort = new EmployeeProfile();
                    userCohort.setUserId(pair._1);
                    cohortList.forEach(cohort -> {
                        CohortInfo cohortInfo = new CohortInfo();
                        cohortInfo.setCohortId(cohort);
                        cohortInfoList.add(cohortInfo);
                    });
                    userCohort.setCohortInfoList(cohortInfoList);
                    countersMap.get(Counters.NUMBER_OF_USERS).add(1L);
                    if (cohortInfoList.size() == 1)
                        countersMap.get(Counters.ONE_COHORT).add(1L);
                    else if (cohortInfoList.size() == 2)
                        countersMap.get(Counters.TWO_COHORTS).add(1L);
                    else if (cohortInfoList.size() == 3)
                        countersMap.get(Counters.THREE_COHORTS).add(1L);
                    else if (cohortInfoList.size() == 4)
                        countersMap.get(Counters.FOUR_COHORTS).add(1L);
                    else if (cohortInfoList.size() == 5)
                        countersMap.get(Counters.FIVE_COHORTS).add(1L);
                    else
                        countersMap.get(Counters.MORE_THAN_FIVE_COHORTS).add(1L);
                    return userCohort;
                });
        return userCohortRDD;
    }

    public static void main(String[] args) {
        SparkApp sparkApp = new SparkApp();
        sparkApp.initSparkConfigs();
        sparkApp.loadConfig(args[1], args[2]);
        sparkApp.process();
        sparkApp.stopAllContext();
    }
}