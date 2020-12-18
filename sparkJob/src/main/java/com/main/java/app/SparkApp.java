package com.main.java.app;

import com.main.java.avro.Employee;
import com.main.java.avro.EmployeeProfile;
import com.main.java.avro.ProfileInfo;
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
 * fetch the data from hdfs and create key:employeeId value:list of profiles
 */
public class SparkApp implements Serializable {

    private JavaSparkContext javaSparkContext;
    private List<String> profileId;
    private String outputDir;
    private static Map<String, LongAccumulator> countersMap;

    /**
     * The complete flow of Spark Job
     */
    private void process() {

        JavaPairRDD<String, String>[] employeeProfileRDDArray = new JavaPairRDD[profileId.size()];
        for (int i = 0; i < profileId.size(); i++) {
            try {
                employeeProfileRDDArray[i] = getEmployeeToProfileRDD(profileId.get(i), ApiUtils.get(Constants.url + profileId.get(i) + "/ACCOUNTID", Constants.xClientIdKey, Constants.xClientIdValue));
            } catch (Exception e) {
                throw new RuntimeException("exception while processing profile id " + profileId.get(i));
            }
        }
        JavaRDD<EmployeeProfile> employeeProfileJavaRDD = convert(javaSparkContext, countersMap, employeeProfileRDDArray);
        writeRDDtoAvro(javaSparkContext, employeeProfileJavaRDD, outputDir);
    }

    /**
     * @param empProfileRDD    rdd consisting of employeeProfile data which is written to HDFS as key empId and value employeeProfileJava
     * @param outputDir        this is the output directory where the data is pushed which is present in config bcuket of airflow
     * @param javaSparkContext This function writes rdd to hdfs path
     */
    private void writeRDDtoAvro(JavaSparkContext javaSparkContext, JavaRDD<EmployeeProfile> empProfileRDD, String outputDir) {

        SparkAvroUtils.setAvroOutputKeyValue(javaSparkContext, Schema.create(Schema.Type.STRING).toString(), EmployeeProfile.getClassSchema().toString());
        JavaPairRDD<AvroKey, AvroValue> employeeProfileAvroPairRDD = empProfileRDD.mapToPair(employeeProfile -> new Tuple2<>(
                new AvroKey<>(employeeProfile.getEmpId()), new AvroValue<>(employeeProfile)));
        SparkAvroUtils.writeAsKeyValueOutputAvro(javaSparkContext, employeeProfileAvroPairRDD, outputDir);
    }

    /**
     * @param profileId     this is the profile Id which is taken from the config bucket profile.prod
     * @param inputFilePath this is the file from where the emloyee data is read for a profile Id
     *                      This function gets the rdd from the inputFilepath
     */
    private JavaPairRDD<String, String> getEmployeeToProfileRDD(String profileId, String inputFilePath) {

        SparkAvroUtils.setAvroInputKeyValue(javaSparkContext, Employee.getClassSchema().toString(), Schema.create(Schema.Type.STRING).toString());
        JavaPairRDD<AvroKey, AvroValue> employeeProfileAvroRDD = SparkAvroUtils.readAsKeyInputAvro(javaSparkContext, inputFilePath);
        return employeeProfileAvroRDD.mapToPair(tuple -> {
            Employee emp = (Employee) tuple._1.datum();
            return new Tuple2<>(emp.getEmpid(), profileId);
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
     * @param profileDict this is the profiles which are fetched from the config bucket profile.prod
     *                    This function gets the profiles from config bucket
     */
    private void getListOfProfiles(Map<String, String> profileDict) {

        setCounters(javaSparkContext);
        profileId = new ArrayList<>();
        for (Map.Entry<String, String> entry : profileDict.entrySet())
            profileId.add(entry.getKey());
        countersMap.get(Counters.NUMBER_OF_PROFILE).add(profileDict.size());
    }

    /**
     * @param bucketName profileId#profileName seperated by _ delimitor
     * @return Map consisting of profileId as key and profileName as value
     * */
    private Map<String, String> getMapFromBucket(String bucketName){

        Map<String, String> bucket = new HashMap<>();
        String[] buckets = bucketName.split("_");
        for(int i=0; i<buckets.length; i++)
            bucket.put(buckets[i].split(Constants.HASH_DELIMITER)[0], buckets[i].split(Constants.HASH_DELIMITER)[1]);
        return bucket;
    }

    /**
     * @param bucketName profile.prod config bucket which contains the profileIds
     * @param outputDir  sets the outputDir where the data is published to HDFS for hawk consumption
     */
    private void loadConfig(String bucketName, String outputDir) {

        Map<String, String> profileDict  = getMapFromBucket(bucketName);
        this.outputDir = outputDir;
        getListOfProfiles(profileDict);
    }

    private void stopAllContext() {

        javaSparkContext.stop();
    }

    /**
     * @param javaSparkContext Creates conters for the emp Profile job
     */
    private void setCounters(JavaSparkContext javaSparkContext) {

        countersMap = Counters.createUCCounters(javaSparkContext);
    }

    /**
     * @param javaSparkContext
     * @param countersMap
     * @param employeeProfileRDDArray this is an array of rdd in which each element is an rdd of empId and profileId
     * @return employeeProfileRDD this is the employeeProfile rdd which is returned consisting of employee profile data
     * This function converts array of rdd which contains the profile, empId pairs to employee to profile rdd
     */
    private JavaRDD<EmployeeProfile> convert(JavaSparkContext javaSparkContext, Map<String, LongAccumulator> countersMap, JavaPairRDD<String, String>[] employeeProfileRDDArray) {

        JavaPairRDD<String, String> unionRDD = javaSparkContext.union(employeeProfileRDDArray);
        JavaRDD<EmployeeProfile> employeeProfileRDD = unionRDD
                .mapToPair(empToProfile -> {
                    countersMap.get(Counters.EMPLOYEE_PROFILE_PAIRS).add(1L);
                    return new Tuple2<>(empToProfile._1, empToProfile._2);
                })
                .groupByKey()
                .map(pair -> {
                    List<String> profileList = new ArrayList<>();
                    List<ProfileInfo> profileInfoList = new ArrayList<>();
                    profileList.addAll((Collection<? extends String>) pair._2);
                    EmployeeProfile employeeProfile = new EmployeeProfile();
                    employeeProfile.setEmpId(pair._1);
                    profileList.forEach(profile -> {
                        ProfileInfo profileInfo = new ProfileInfo();
                        profileInfo.setProfileId(profile);
                        profileInfoList.add(profileInfo);
                    });
                    employeeProfile.setProfileInfoList(profileInfoList);
                    countersMap.get(Counters.NUMBER_OF_EMPLOYEE).add(1L);
                    if (profileInfoList.size() == 1)
                        countersMap.get(Counters.ONE_PROFILE).add(1L);
                    else if (profileInfoList.size() == 2)
                        countersMap.get(Counters.TWO_PROFILES).add(1L);
                    else if (profileInfoList.size() == 3)
                        countersMap.get(Counters.THREE_PROFILES).add(1L);
                    else if (profileInfoList.size() == 4)
                        countersMap.get(Counters.FOUR_PROFILES).add(1L);
                    else if (profileInfoList.size() == 5)
                        countersMap.get(Counters.FIVE_PROFILES).add(1L);
                    else
                        countersMap.get(Counters.MORE_THAN_FIVE_PROFILES).add(1L);
                    return employeeProfile;
                });
        return employeeProfileRDD;
    }

    public static void main(String[] args) {

        SparkApp sparkApp = new SparkApp();
        String bucketName = args[1];
        String outputDir = args[2];
        sparkApp.initSparkConfigs();
        sparkApp.loadConfig(bucketName, outputDir);
        sparkApp.process();
        sparkApp.stopAllContext();
    }
}