# distributed-big-data-systems
Spark Job.java: fetches Avro data from HDFS in the format path key contains values. From different HDFS paths fetch input and combine them and dumps data in the format of value1->key1, value2->key2 Avro format in HDFS and in Kafka.

KafkaPublisher.java: fetches data from HDFS to publish it to Kafka.
(this was implemented seperately to reduce parallelism while publishing to Kafka).

Employee.avsc: input Avro format.

EmployeeProfile.avsc: output Avro format.

PushToKafka.java: Pushes data to Kafka as key, value pair (not implemented).

ApiUtils.java: Uses GET request to make API calls to fetch data.

SparkAvroUtils.java: Sets Spark Avro format for input and output formats.

Command to run the Spark Job

/home/fk-reco/sparkhome/bin/spark-submit  --master yarn --conf "-XX:+UnlockCommercialFeatures -XX:+FlightRecorder -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=0 -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 -XX:+DisableExplicitGC -XX:+UnlockDiagnosticVMOptions -XX:ParGCCardsPerStrideChunk=16384 -XX:InitialTenuringThreshold=10 -XX:+ParallelRefProcEnabled -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=50 -XX:+CMSClassUnloadingEnabled -XX:+CMSPermGenSweepingEnabled -XX:+CMSParallelRemarkEnabled -XX:+CMSScavengeBeforeRemark -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:+DebugNonSafepoints -Djava.io.tmpdir=/rahul/tmp/" --executor-cores 4 --num-executors 20 --name EmployeeProfile --driver-memory 8G --executor-memory 30G --class com.main.java.app.SparkApp --queue queuename --deploy-mode cluster FileName.jar arguments

Airflow Dag to run the Spark Job


