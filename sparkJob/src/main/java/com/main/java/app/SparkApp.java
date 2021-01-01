package com.main.java.app;

import com.main.java.avro.CategoryInfo;
import com.main.java.avro.Employee;
import com.main.java.avro.EmployeeCategory;
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
 * fetch the data from HDFS and create key:employeeId value:list of category
 */
public class SparkApp implements Serializable {

    private JavaSparkContext javaSparkContext;
    private List<String> categoryId;
    private String outputDir;
    private static Map<String, LongAccumulator> countersMap;

    /**
     * The complete flow of Spark Job
     */
    private void process() {

        JavaPairRDD<String, String>[] employeeProfileRDDArray = new JavaPairRDD[categoryId.size()];
        for (int i = 0; i < categoryId.size(); i++) {
            try {
                employeeProfileRDDArray[i] = getEmployeeToCategoryRDD(categoryId.get(i), ApiUtils.get(Constants.url + categoryId.get(i) + "/ACCOUNTID", Constants.xClientIdKey, Constants.xClientIdValue));
            } catch (Exception e) {
                throw new RuntimeException("exception while processing category id " + categoryId.get(i));
            }
        }
        JavaRDD<EmployeeCategory> employeeProfileJavaRDD = convert(javaSparkContext, countersMap, employeeProfileRDDArray);
        writeRDDtoAvro(javaSparkContext, employeeProfileJavaRDD, outputDir);
    }

    /**
     * @param empCategoryRDD    rdd consisting of employeeProfile data which is written to HDFS as key empId and value employeeProfileJava
     * @param outputDir        this is the output directory where the data is pushed which is present in config bcuket of airflow
     * @param javaSparkContext This function writes rdd to hdfs path
     */
    private void writeRDDtoAvro(JavaSparkContext javaSparkContext, JavaRDD<EmployeeCategory> empCategoryRDD, String outputDir) {

        SparkAvroUtils.setAvroOutputKeyValue(javaSparkContext, Schema.create(Schema.Type.STRING).toString(), EmployeeCategory.getClassSchema().toString());
        JavaPairRDD<AvroKey, AvroValue> employeeProfileAvroPairRDD = empCategoryRDD.mapToPair(employeeProfile -> new Tuple2<>(
                new AvroKey<>(employeeProfile.getEmpId()), new AvroValue<>(employeeProfile)));
        SparkAvroUtils.writeAsKeyValueOutputAvro(javaSparkContext, employeeProfileAvroPairRDD, outputDir);
    }

    /**
     * @param categoryId  this is the category Id which is taken from the config bucket category.prod
     * @param inputFilePath this is the file from where the employee data is read for a category Id
     *                      This function gets the rdd from the inputFilepath
     */
    private JavaPairRDD<String, String> getEmployeeToCategoryRDD(String categoryId, String inputFilePath) {

        SparkAvroUtils.setAvroInputKeyValue(javaSparkContext, Employee.getClassSchema().toString(), Schema.create(Schema.Type.STRING).toString());
        JavaPairRDD<AvroKey, AvroValue> employeeCategoryAvroRDD = SparkAvroUtils.readAsKeyInputAvro(javaSparkContext, inputFilePath);
        return employeeCategoryAvroRDD.mapToPair(tuple -> {
            Employee emp = (Employee) tuple._1.datum();
            return new Tuple2<>(emp.getEmpid(), categoryId);
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
     * @param categoryDict this is the category which are fetched from the config bucket category.prod
     *                    This function gets the category from config bucket
     */
    private void getListOfProfiles(Map<String, String> categoryDict) {

        setCounters(javaSparkContext);
        categoryId = new ArrayList<>();
        for (Map.Entry<String, String> entry : categoryDict.entrySet())
            categoryId.add(entry.getKey());
        countersMap.get(Counters.NUMBER_OF_CATEGORY).add(categoryDict.size());
    }

    /**
     * @param bucketName categoryId#categoryName seperated by _ delimitor
     * @return Map consisting of categoryId as key and categoryName as value
     * */
    private Map<String, String> getMapFromBucket(String bucketName) {

        Map<String, String> bucket = new HashMap<>();
        String[] buckets = bucketName.split("_");
        for(int i=0; i<buckets.length; i++)
            bucket.put(buckets[i].split(Constants.HASH_DELIMITER)[0], buckets[i].split(Constants.HASH_DELIMITER)[1]);
        return bucket;
    }

    /**
     * @param bucketName category.prod config bucket which contains the categoryIds
     * @param outputDir  sets the outputDir where the data is published to HDFS for hawk consumption
     */
    private void loadConfig(String bucketName, String outputDir) {

        Map<String, String> categoryDict  = getMapFromBucket(bucketName);
        this.outputDir = outputDir;
        getListOfProfiles(categoryDict);
    }

    private void stopAllContext() {

        javaSparkContext.stop();
    }

    /**
     * @param javaSparkContext Creates counters for the emp Profile job
     */
    private void setCounters(JavaSparkContext javaSparkContext) {

        countersMap = Counters.createUCCounters(javaSparkContext);
    }

    /**
     * @param javaSparkContext
     * @param countersMap
     * @param employeeProfileRDDArray this is an array of rdd in which each element is an rdd of empId and categoryId
     * @return employeeProfileRDD this is the employeeProfile rdd which is returned consisting of employee category data
     * This function converts array of rdd which contains the category, empId pairs to employee to category rdd
     */
    private JavaRDD<EmployeeCategory> convert(JavaSparkContext javaSparkContext, Map<String, LongAccumulator> countersMap, JavaPairRDD<String, String>[] employeeProfileRDDArray) {

        JavaPairRDD<String, String> unionRDD = javaSparkContext.union(employeeProfileRDDArray);
        JavaRDD<EmployeeCategory> employeeProfileRDD = unionRDD
                .mapToPair(empToProfile -> {
                    countersMap.get(Counters.EMPLOYEE_CATEGORY_PAIRS).add(1L);
                    return new Tuple2<>(empToProfile._1, empToProfile._2);
                })
                .groupByKey()
                .map(pair -> {
                    List<String> categoryList = new ArrayList<>();
                    List<CategoryInfo> categoryInfoList = new ArrayList<>();
                    categoryList.addAll((Collection<? extends String>) pair._2);
                    EmployeeCategory employeeProfile = new EmployeeCategory();
                    employeeProfile.setEmpId(pair._1);
                    categoryList.forEach(category -> {
                        CategoryInfo categoryInfo = new CategoryInfo();
                        categoryInfo.setCategoryId(category);
                        categoryInfoList.add(categoryInfo);
                    });
                    employeeProfile.setCategoryInfoList(categoryInfoList);
                    countersMap.get(Counters.NUMBER_OF_EMPLOYEE).add(1L);
                    if (categoryInfoList.size() == 1)
                        countersMap.get(Counters.ONE_CATEGORY).add(1L);
                    else if (categoryInfoList.size() == 2)
                        countersMap.get(Counters.TWO_CATEGORIES).add(1L);
                    else if (categoryInfoList.size() == 3)
                        countersMap.get(Counters.THREE_CATEGORIES).add(1L);
                    else if (categoryInfoList.size() == 4)
                        countersMap.get(Counters.FOUR_CATEGORIES).add(1L);
                    else if (categoryInfoList.size() == 5)
                        countersMap.get(Counters.FIVE_CATEGORIES).add(1L);
                    else
                        countersMap.get(Counters.MORE_THAN_FIVE_CATEGORIES).add(1L);
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